import asyncio
import websockets
import json
import re
import time
import os
import sys
import base64
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# See flipper_variant_a.py for why this is needed: emoji in status/log
# messages raise UnicodeEncodeError under this console's default codepage,
# which aborts the connection before write_audit_log() runs.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# --- CONFIGURATION ---
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_REST_URL = "https://api.elections.kalshi.com/trade-api/v2"
RECONNECT_COOLDOWN_SECONDS = 30
HOLD_EXPIRATION_SECONDS = 120

# --- BOT C STRATEGY PARAMETERS ---
MOVE_THRESHOLD = 15.00
MIN_ASK_CENTS = 42
MAX_ASK_CENTS = 58
PROFIT_TARGET_CENTS = 15
STOP_LOSS_CENTS = 12
ACTIVE_PHASE_CUTOFF = 300  # First 5 minutes of the 15m window
MAX_TRADES_PER_WINDOW = 4  # Re-arms after each close instead of sitting out the rest of the window
STATUS_UPDATE_INTERVAL = 15

load_dotenv()
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")

def load_private_key(file_path):
    with open(file_path, "rb") as key_file:
        return serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )

def extract_strike_price(market_obj):
    """
    Kalshi's KXBTC15M markets occasionally list without a strike attached yet
    (observed historically as "Strike: $0.00" in the audit log) -- the
    is_trading_allowed() zero-strike guard exists specifically to refuse
    trading against those.
    """
    strike = market_obj.get("floor_strike") or market_obj.get("cap_strike")
    if strike and float(strike) > 0:
        return float(strike)

    text_to_search = f"{market_obj.get('title', '')} {market_obj.get('subtitle', '')}"
    matches = re.findall(r"\$?([0-9]{2,3},?[0-9]{3}\.?[0-9]*)", text_to_search)
    if matches:
        try:
            return float(matches[0].replace(",", ""))
        except ValueError:
            pass
    return 0.0

class BotC:
    def __init__(self):
        self.bot_name = "BOT C - US HOURS & WIN SCALER"
        self.last_reconnect_time = 0.0

        # Position Management
        self.active_position = None
        self.position_start_time = 0.0

        # Window & Strategy Management
        self.private_key = load_private_key(KALSHI_PRIVATE_KEY_PATH)
        self.current_window_id = ""
        self.window_start_timestamp = 0.0
        self.current_strike = 0.0
        self.trade_count = 0
        self.brti_anchor_price = 0.0
        self.current_brti = 0.0

        self.last_status_time = 0.0

    def write_audit_log(self, message):
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        log_entry = f"[{timestamp}] {message}\n"
        filename = f"{self.bot_name.replace(' ', '_')}_audit.log"
        with open(filename, "a", encoding="utf-8") as f:
            f.write(log_entry)

    def sign(self, method, path):
        """Signs a request path (query string excluded) for Kalshi's HMAC auth headers."""
        timestamp = str(int(time.time() * 1000))
        path_clean = path.split('?')[0]
        message = f"{timestamp}{method}{path_clean}".encode('utf-8')
        signature_bytes = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        signature = base64.b64encode(signature_bytes).decode('utf-8')
        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp
        }

    def get_live_market(self):
        """
        REST-selects the currently open KXBTC15M market (earliest close_time) and
        returns (ticker, window_start_timestamp, strike). window_start_timestamp
        comes from the REST "open_time" field (authoritative UTC) -- see
        flipper_variant_a.py for why parsing it out of the ticker string is wrong.
        """
        sign_path = "/trade-api/v2/markets"
        query = "?limit=5&status=open&series_ticker=KXBTC15M"
        headers = self.sign("GET", sign_path)
        try:
            res = requests.get(f"{KALSHI_REST_URL}/markets{query}", headers=headers, timeout=10)
            if res.status_code != 200:
                return None
            markets = res.json().get("markets", [])
            if not markets:
                return None
            markets.sort(key=lambda m: m.get("close_time", ""))
            market = markets[0]
            open_time = datetime.strptime(market["open_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return market["ticker"], open_time.timestamp(), extract_strike_price(market)
        except (requests.RequestException, KeyError, ValueError):
            return None

    def is_in_us_session(self):
        """Shifted start time to 15:30 UTC to avoid the open chop."""
        now = datetime.now(timezone.utc).time()
        start_time = datetime.strptime("15:30", "%H:%M").time()
        end_time = datetime.strptime("21:00", "%H:%M").time()
        return start_time <= now <= end_time

    def is_trading_allowed(self, strike_price):
        if not self.is_in_us_session():
            return False
        if strike_price == 0.0 or strike_price is None:
            return False
        if time.time() - self.last_reconnect_time < RECONNECT_COOLDOWN_SECONDS:
            return False
        return True

    async def run(self):
        startup_msg = f"ENGINE STARTED [{self.bot_name}] | Hours: 15:30-21:00 UTC | Move: ${MOVE_THRESHOLD}"
        print(f"[INFO] {startup_msg}")
        self.write_audit_log(startup_msg)

        while True:
            try:
                live_market = await asyncio.to_thread(self.get_live_market)
                if not live_market:
                    await asyncio.sleep(2)
                    continue
                target_ticker, target_start_timestamp, target_strike = live_market

                # Right at a window boundary, REST can briefly still report the
                # just-closed market as "open" -- locking onto it means no more
                # ticks will ever arrive for it, and the bot would just hang.
                if time.time() - target_start_timestamp >= 900:
                    await asyncio.sleep(2)
                    continue

                is_new_window = target_ticker != self.current_window_id
                if is_new_window:
                    self.current_window_id = target_ticker
                    self.window_start_timestamp = target_start_timestamp
                    self.current_strike = target_strike
                    self.trade_count = 0
                    self.brti_anchor_price = 0.0
                    self.active_position = None

                self.last_reconnect_time = time.time()
                ws_headers = self.sign("GET", "/trade-api/ws/v2")

                async with websockets.connect(KALSHI_WS_URL, ping_interval=None, additional_headers=ws_headers) as ws:
                    print(f"[INFO] {self.bot_name} connected to WebSocket. Locked onto window {target_ticker}.")

                    # Scope the ticker channel to this one market -- an unscoped
                    # subscription streams ticks for every market on the exchange
                    # and effectively never surfaces KXBTC15M updates in practice.
                    subscribe_ticker_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["ticker"],
                            "market_tickers": [target_ticker]
                        }
                    }
                    await ws.send(json.dumps(subscribe_ticker_msg))

                    subscribe_index_msg = {
                        "id": 2,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["cfbenchmarks_value"],
                            "index_ids": ["BRTI"]
                        }
                    }
                    await ws.send(json.dumps(subscribe_index_msg))
                    print(f"[INFO] {self.bot_name} sent subscription requests for tickers and BTC index.")

                    async for message in ws:
                        data = json.loads(message)
                        msg_type = data.get("type", "unknown")

                        if msg_type == "ping" or message == "heartbeat":
                            await ws.send("pong")
                            continue

                        # --- 1. CAPTURE THE BTC INDEX PRICE ---
                        if msg_type == "cfbenchmarks_value":
                            try:
                                msg_obj = data.get("msg", {})
                                raw_data_str = msg_obj.get("data", "{}")
                                parsed_index_data = json.loads(raw_data_str)
                                self.current_brti = float(parsed_index_data.get("value", 0.0))

                                if self.brti_anchor_price == 0.0 and self.current_brti > 0:
                                    self.brti_anchor_price = self.current_brti
                                    if self.trade_count == 0:
                                        open_msg = f"📍 NEW WINDOW: {self.current_window_id} | INITIAL BRTI ANCHOR: ${self.brti_anchor_price:,.2f}"
                                    else:
                                        open_msg = f"🔄 RE-ARMED: {self.current_window_id} | NEW ANCHOR: ${self.brti_anchor_price:,.2f} | Trades So Far: {self.trade_count}"
                                    print(f"\n[INFO] {open_msg}")
                                    self.write_audit_log(open_msg)
                            except (json.JSONDecodeError, ValueError, TypeError):
                                pass

                        # --- 2. CAPTURE THE MARKET TICKER & EXECUTE LOGIC ---
                        elif msg_type == "ticker":
                            msg = data.get("msg", {})
                            market_ticker = msg.get("market_ticker", "")

                            if market_ticker != self.current_window_id:
                                continue

                            yes_bid = int(float(msg.get("yes_bid_dollars", "0.0")) * 100)
                            yes_ask = int(float(msg.get("yes_ask_dollars", "0.0")) * 100)
                            no_bid = 100 - yes_ask if yes_ask > 0 else 0
                            no_ask = 100 - yes_bid if yes_bid > 0 else 100

                            elapsed_time = time.time() - self.window_start_timestamp

                            # Heartbeat status update (every 15 seconds)
                            if time.time() - self.last_status_time >= STATUS_UPDATE_INTERVAL:
                                if elapsed_time <= ACTIVE_PHASE_CUTOFF and self.trade_count < MAX_TRADES_PER_WINDOW:
                                    time_left = int(ACTIVE_PHASE_CUTOFF - elapsed_time)
                                    current_delta = self.current_brti - self.brti_anchor_price
                                    session_tag = "US SESSION" if self.is_in_us_session() else "OUTSIDE SESSION"
                                    print(f"[STATUS] ⏳ Entry Phase Active ({session_tag}) | BTC: ${self.current_brti:,.2f} | Delta: ${current_delta:+.2f} | Entry Time Left: {time_left}s")
                                elif elapsed_time > ACTIVE_PHASE_CUTOFF:
                                    print(f"[STATUS] 💤 Entry Phase Closed (Watching Market) | BTC: ${self.current_brti:,.2f}")
                                self.last_status_time = time.time()

                            # Forced exit once the active phase closes, mirroring the
                            # proven historical behavior (see audit_c_us_session-old.log).
                            if elapsed_time >= ACTIVE_PHASE_CUTOFF and self.active_position:
                                side = self.active_position
                                bid = yes_bid if side == "YES" else no_bid
                                pl = bid - self.entry_price
                                exit_msg = f"⏱️ ACTIVE PHASE CUTOFF ({ACTIVE_PHASE_CUTOFF}s): Forced exit {side} @ {bid}c (P/L: {pl:+d}c)"
                                print(f"[INFO] {exit_msg}")
                                self.write_audit_log(exit_msg)
                                self.active_position = None

                            # Window rollover -- break out to reconnect scoped to the next market.
                            if elapsed_time >= 900:
                                close_msg = f"🏁 WINDOW EXPIRED | Window: {self.current_window_id} | Total Trades: {self.trade_count}"
                                print(f"[INFO] {close_msg}")
                                self.write_audit_log(close_msg)
                                break

                            # 3. Trigger Calculation
                            if (self.brti_anchor_price > 0 and self.active_position is None
                                    and elapsed_time < ACTIVE_PHASE_CUTOFF and self.trade_count < MAX_TRADES_PER_WINDOW):
                                delta = self.current_brti - self.brti_anchor_price

                                if abs(delta) >= MOVE_THRESHOLD and self.is_trading_allowed(self.current_strike):

                                    if delta > 0 and (MIN_ASK_CENTS <= yes_ask <= MAX_ASK_CENTS):
                                        self.active_position = "YES"
                                        self.entry_price = yes_ask
                                        self.target_sell = yes_ask + PROFIT_TARGET_CENTS
                                        self.stop_price = max(1, yes_ask - STOP_LOSS_CENTS)
                                        self.position_start_time = time.time()
                                        self.trade_count += 1

                                        trade_msg = (
                                            f"🎯 ENTRY #{self.trade_count}: Bought YES @ {yes_ask}c | BTC Delta: $+{delta:.2f} | "
                                            f"Target: {self.target_sell}c | Stop: {self.stop_price}c"
                                        )
                                        print(f"[INFO] {trade_msg}")
                                        self.write_audit_log(trade_msg)

                                    elif delta < 0 and (MIN_ASK_CENTS <= no_ask <= MAX_ASK_CENTS):
                                        self.active_position = "NO"
                                        self.entry_price = no_ask
                                        self.target_sell = no_ask + PROFIT_TARGET_CENTS
                                        self.stop_price = max(1, no_ask - STOP_LOSS_CENTS)
                                        self.position_start_time = time.time()
                                        self.trade_count += 1

                                        trade_msg = (
                                            f"🎯 ENTRY #{self.trade_count}: Bought NO @ {no_ask}c | BTC Delta: ${delta:.2f} | "
                                            f"Target: {self.target_sell}c | Stop: {self.stop_price}c"
                                        )
                                        print(f"[INFO] {trade_msg}")
                                        self.write_audit_log(trade_msg)

                                    # Re-arm: next tick's BRTI value becomes the fresh
                                    # anchor so the bot can catch the next swing from
                                    # here, instead of needing an even bigger
                                    # cumulative move from the original window-open price.
                                    self.brti_anchor_price = 0.0

                            # 4. Position management: take profit / stop loss / hold expiration
                            if self.active_position:
                                side = self.active_position
                                bid = yes_bid if side == "YES" else no_bid
                                hold_time = time.time() - self.position_start_time

                                if bid >= self.target_sell:
                                    pl = bid - self.entry_price
                                    exit_msg = f"✅ TAKE PROFIT: Sold {side} @ {bid}c (Gain: +{pl}c)"
                                    print(f"[INFO] {exit_msg}")
                                    self.write_audit_log(exit_msg)
                                    self.active_position = None
                                    self.brti_anchor_price = 0.0

                                elif bid <= self.stop_price and bid > 0:
                                    pl = bid - self.entry_price
                                    exit_msg = f"🛑 STOP LOSS: Sold {side} @ {bid}c (Loss: {pl}c)"
                                    print(f"[INFO] {exit_msg}")
                                    self.write_audit_log(exit_msg)
                                    self.active_position = None
                                    self.brti_anchor_price = 0.0

                                elif hold_time >= HOLD_EXPIRATION_SECONDS:
                                    pl = bid - self.entry_price
                                    exit_msg = f"⏱️ HOLD EXPIRED ({HOLD_EXPIRATION_SECONDS}s): Closed {side} @ {bid}c (P/L: {pl:+d}c)"
                                    print(f"[INFO] {exit_msg}")
                                    self.write_audit_log(exit_msg)
                                    self.active_position = None
                                    self.brti_anchor_price = 0.0

            except Exception as e:
                err_msg = f"Exception: {e}. Reconnecting in 5s..."
                print(f"[ERROR] {self.bot_name} {err_msg}")
                self.write_audit_log(f"ERROR: {err_msg}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    bot = BotC()
    asyncio.run(bot.run())
