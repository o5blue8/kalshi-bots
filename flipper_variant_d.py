import asyncio
import websockets
import json
import re
import time
import os
import sys
import base64
import requests
from collections import deque
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

# --- BOT D STRATEGY PARAMETERS ---
# Unlike A/B/C (single-timeframe trigger, tight scalp exits), Bot D requires
# a shorter AND a longer lookback to agree on direction before entering --
# a single-tick spike that reverses in 10-30s (the US/London-open fakeout
# pattern that hurt A/B/C) won't clear the 180s bar even if it clears the 60s
# one. In exchange for being more selective, it rides a confirmed move
# further: wider target/stop, one trade per window, no short fixed hold --
# it exits on target, stop, or the hard pre-settlement cutoff, whichever
# comes first.
MIN_ASK_CENTS = 40
MAX_ASK_CENTS = 60
PROFIT_TARGET_CENTS = 30
STOP_LOSS_CENTS = 18
MOVE_THRESHOLD_60S = 20.0
MOVE_THRESHOLD_180S = 35.0
CONFIRMATION_LOOKBACK_SECONDS = 180
ENTRY_CUTOFF_SECONDS = 660   # No new entries after 11 minutes into the window
FORCE_EXIT_SECONDS = 840     # Any open position is force-closed by 14 minutes in,
                             # regardless of P/L -- never rides into settlement.
MAX_TRADES_PER_WINDOW = 1    # Patient/selective: one confirmed swing per window
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

class BotD:
    def __init__(self):
        self.bot_name = "BOT D - SWING CONFIRMATION"
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
        self.current_brti = 0.0
        self.price_history = deque()  # (timestamp, brti) samples, pruned to CONFIRMATION_LOOKBACK_SECONDS

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

    def price_n_seconds_ago(self, n):
        """Earliest sampled price at least n seconds old -- None if not enough history yet."""
        cutoff = time.time() - n
        for ts, price in self.price_history:
            if ts <= cutoff:
                continue
            return price
        return None

    def is_blackout_window(self):
        now = datetime.now(timezone.utc).time()
        # London open. Left as-is from the original design.
        if now >= datetime.strptime("07:30", "%H:%M").time() and now <= datetime.strptime("09:00", "%H:%M").time():
            return True
        # US cash-equity session (13:00-20:00 UTC). The Aug 2026 burn-in showed
        # losses clustered hard in 15:00-20:00 UTC (US afternoon) across every
        # bot and all days; the old blackout ended at 15:30, right before the
        # worst hours (16:00-19:00 UTC). Extended to cover the full US session.
        if now >= datetime.strptime("13:00", "%H:%M").time() and now <= datetime.strptime("20:00", "%H:%M").time():
            return True
        return False

    def is_trading_allowed(self, strike_price):
        if self.is_blackout_window():
            return False
        if strike_price == 0.0 or strike_price is None:
            return False
        if time.time() - self.last_reconnect_time < RECONNECT_COOLDOWN_SECONDS:
            return False
        return True

    async def run(self):
        startup_msg = (
            f"ENGINE STARTED [{self.bot_name}] | 60s Move: ${MOVE_THRESHOLD_60S} | "
            f"180s Move: ${MOVE_THRESHOLD_180S} | Target: +{PROFIT_TARGET_CENTS}c | Stop: -{STOP_LOSS_CENTS}c"
        )
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
                    self.active_position = None
                    self.price_history.clear()

                self.last_reconnect_time = time.time()
                ws_headers = self.sign("GET", "/trade-api/ws/v2")

                async with websockets.connect(KALSHI_WS_URL, ping_interval=None, additional_headers=ws_headers) as ws:
                    print(f"[INFO] {self.bot_name} connected to WebSocket. Locked onto window {target_ticker}.")

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
                                if self.current_brti > 0:
                                    now_t = time.time()
                                    self.price_history.append((now_t, self.current_brti))
                                    prune_before = now_t - CONFIRMATION_LOOKBACK_SECONDS - 5
                                    while self.price_history and self.price_history[0][0] < prune_before:
                                        self.price_history.popleft()
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
                                p60 = self.price_n_seconds_ago(60)
                                p180 = self.price_n_seconds_ago(180)
                                d60 = (self.current_brti - p60) if p60 else 0.0
                                d180 = (self.current_brti - p180) if p180 else 0.0
                                if self.active_position:
                                    print(f"[STATUS] 📈 Holding {self.active_position} | BTC: ${self.current_brti:,.2f} | Elapsed: {int(elapsed_time)}s")
                                elif elapsed_time <= ENTRY_CUTOFF_SECONDS and self.trade_count < MAX_TRADES_PER_WINDOW:
                                    print(f"[STATUS] ⏳ Watching | BTC: ${self.current_brti:,.2f} | 60s Δ: ${d60:+.2f} | 180s Δ: ${d180:+.2f} | Entry Time Left: {int(ENTRY_CUTOFF_SECONDS - elapsed_time)}s")
                                else:
                                    print(f"[STATUS] 💤 Entry Window Closed (Watching Market) | BTC: ${self.current_brti:,.2f}")
                                self.last_status_time = time.time()

                            # Hard pre-settlement cutoff -- force-close regardless of P/L.
                            if elapsed_time >= FORCE_EXIT_SECONDS and self.active_position:
                                side = self.active_position
                                bid = yes_bid if side == "YES" else no_bid
                                pl = bid - self.entry_price
                                exit_msg = f"⏱️ FORCE EXIT ({FORCE_EXIT_SECONDS}s pre-settlement): Sold {side} @ {bid}c (P/L: {pl:+d}c)"
                                print(f"[INFO] {exit_msg}")
                                self.write_audit_log(exit_msg)
                                self.active_position = None

                            # Window rollover -- break out to reconnect scoped to the next market.
                            if elapsed_time >= 900:
                                close_msg = f"🏁 WINDOW EXPIRED | Window: {self.current_window_id} | Total Trades: {self.trade_count}"
                                print(f"[INFO] {close_msg}")
                                self.write_audit_log(close_msg)
                                break

                            # 3. Trigger Calculation -- 60s AND 180s deltas must agree in
                            # direction and both clear their threshold. A spike that only
                            # clears the 60s bar (no follow-through) is filtered out here.
                            if (self.active_position is None and elapsed_time < ENTRY_CUTOFF_SECONDS
                                    and self.trade_count < MAX_TRADES_PER_WINDOW):
                                p60 = self.price_n_seconds_ago(60)
                                p180 = self.price_n_seconds_ago(180)

                                if p60 is not None and p180 is not None:
                                    delta_60 = self.current_brti - p60
                                    delta_180 = self.current_brti - p180
                                    confirmed_up = delta_60 >= MOVE_THRESHOLD_60S and delta_180 >= MOVE_THRESHOLD_180S
                                    confirmed_down = delta_60 <= -MOVE_THRESHOLD_60S and delta_180 <= -MOVE_THRESHOLD_180S

                                    if (confirmed_up or confirmed_down) and self.is_trading_allowed(self.current_strike):

                                        if confirmed_up and (MIN_ASK_CENTS <= yes_ask <= MAX_ASK_CENTS):
                                            self.active_position = "YES"
                                            self.entry_price = yes_ask
                                            self.target_sell = yes_ask + PROFIT_TARGET_CENTS
                                            self.stop_price = max(1, yes_ask - STOP_LOSS_CENTS)
                                            self.position_start_time = time.time()
                                            self.trade_count += 1

                                            trade_msg = (
                                                f"🎯 ENTRY #{self.trade_count}: Bought YES @ {yes_ask}c | "
                                                f"60s Δ: $+{delta_60:.2f} | 180s Δ: $+{delta_180:.2f} | "
                                                f"Target: {self.target_sell}c | Stop: {self.stop_price}c"
                                            )
                                            print(f"[INFO] {trade_msg}")
                                            self.write_audit_log(trade_msg)

                                        elif confirmed_down and (MIN_ASK_CENTS <= no_ask <= MAX_ASK_CENTS):
                                            self.active_position = "NO"
                                            self.entry_price = no_ask
                                            self.target_sell = no_ask + PROFIT_TARGET_CENTS
                                            self.stop_price = max(1, no_ask - STOP_LOSS_CENTS)
                                            self.position_start_time = time.time()
                                            self.trade_count += 1

                                            trade_msg = (
                                                f"🎯 ENTRY #{self.trade_count}: Bought NO @ {no_ask}c | "
                                                f"60s Δ: ${delta_60:.2f} | 180s Δ: ${delta_180:.2f} | "
                                                f"Target: {self.target_sell}c | Stop: {self.stop_price}c"
                                            )
                                            print(f"[INFO] {trade_msg}")
                                            self.write_audit_log(trade_msg)

                            # 4. Position management -- target / stop only. No short fixed
                            # hold: the whole point is to let a confirmed move develop,
                            # up to the hard FORCE_EXIT_SECONDS cutoff above.
                            if self.active_position:
                                side = self.active_position
                                bid = yes_bid if side == "YES" else no_bid

                                if bid >= self.target_sell:
                                    pl = bid - self.entry_price
                                    exit_msg = f"✅ TAKE PROFIT: Sold {side} @ {bid}c (Gain: +{pl}c)"
                                    print(f"[INFO] {exit_msg}")
                                    self.write_audit_log(exit_msg)
                                    self.active_position = None

                                elif bid <= self.stop_price and bid > 0:
                                    pl = bid - self.entry_price
                                    exit_msg = f"🛑 STOP LOSS: Sold {side} @ {bid}c (Loss: {pl}c)"
                                    print(f"[INFO] {exit_msg}")
                                    self.write_audit_log(exit_msg)
                                    self.active_position = None

            except Exception as e:
                err_msg = f"Exception: {e}. Reconnecting in 5s..."
                print(f"[ERROR] {self.bot_name} {err_msg}")
                self.write_audit_log(f"ERROR: {err_msg}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    bot = BotD()
    asyncio.run(bot.run())
