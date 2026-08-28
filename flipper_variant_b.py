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
from collections import deque
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
HOLD_EXPIRATION_SECONDS = 45  # REDUCED heavily for momentum strategy
RECONNECT_COOLDOWN_SECONDS = 30
CHOP_THRESHOLD_MULTIPLIER = 1.5 # Require 1.5x standard momentum if chopped

# --- BOT B STRATEGY PARAMETERS ---
MIN_ENTRY_PRICE_CENTS = 40
MAX_ENTRY_PRICE_CENTS = 60
PROFIT_TARGET_CENTS = 15
STOP_LOSS_CENTS = 12
ACTIVE_PHASE_CUTOFF = 600      # Active for first 10 minutes of window
MIN_BTC_MOVE_USD = 25.0        # Demands a $25 move within the lookback window
LOOKBACK_SECONDS = 60          # 60-second rolling lookback window
MAX_TRADES_PER_WINDOW = 4      # Re-arms after each close instead of sitting out the rest of the window

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
    trading against those. Falls back to parsing the strike out of the
    title/subtitle if floor_strike/cap_strike aren't populated.
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

class BotB:
    def __init__(self):
        self.bot_name = "BOT B - ROLLING BREAKOUT"
        self.last_reconnect_time = 0.0

        # Position Management
        self.active_position = None
        self.position_start_time = 0.0
        self.recent_losses = 0

        # Window & Strategy Management
        self.private_key = load_private_key(KALSHI_PRIVATE_KEY_PATH)
        self.current_window_id = ""
        self.window_start_timestamp = 0.0
        self.current_strike = 0.0
        self.trade_count = 0
        self.current_brti = 0.0
        self.price_history = deque()  # (timestamp, brti) samples over the lookback window

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
        returns (ticker, window_start_timestamp). window_start_timestamp comes
        from the REST "open_time" field (authoritative UTC) -- see
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

    def get_market_orderbook(self, ticker):
        sign_path = f"/trade-api/v2/markets/{ticker}/orderbook"
        headers = self.sign("GET", sign_path)
        try:
            res = requests.get(f"{KALSHI_REST_URL}/markets/{ticker}/orderbook", headers=headers, timeout=10)
            if res.status_code != 200:
                return None
            data = res.json()
            ob = data.get("orderbook_fp", data.get("orderbook", {}))
            yes_bids = [float(x[0]) for x in (ob.get("yes_dollars") or ob.get("yes") or []) if x]
            no_bids = [float(x[0]) for x in (ob.get("no_dollars") or ob.get("no") or []) if x]
            best_yes_bid = max(yes_bids) if yes_bids else 0.0
            best_no_bid = max(no_bids) if no_bids else 0.0
            yes_ask = (1.0 - best_no_bid) if best_no_bid > 0 else 1.00
            no_ask = (1.0 - best_yes_bid) if best_yes_bid > 0 else 1.00
            return {
                "yes_bid_cents": int(round(best_yes_bid * 100)),
                "yes_ask_cents": int(round(yes_ask * 100)),
                "no_bid_cents": int(round(best_no_bid * 100)),
                "no_ask_cents": int(round(no_ask * 100))
            }
        except (requests.RequestException, ValueError, KeyError):
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

    def is_trading_allowed(self, strike_price, current_momentum, base_threshold):
        """Includes the Chop Detector requirement"""
        if strike_price == 0.0 or strike_price is None:
            return False

        if self.is_blackout_window():
            return False

        if time.time() - self.last_reconnect_time < RECONNECT_COOLDOWN_SECONDS:
            return False

        # Volatility Filter / Chop Detector: after recent losses, demand a
        # higher momentum threshold to enter.
        required_momentum = base_threshold
        if self.recent_losses >= 2:
            required_momentum = base_threshold * CHOP_THRESHOLD_MULTIPLIER

        if current_momentum < required_momentum:
            return False

        return True

    async def run(self):
        startup_msg = f"ENGINE STARTED [{self.bot_name}] | Target Move: ${MIN_BTC_MOVE_USD} in {LOOKBACK_SECONDS}s"
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
                # just-closed market as "open" -- locking onto it would mean no
                # more BRTI-driven activity ever closes this "window" out.
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

                # ping_interval=None + manual pong on server "ping" messages,
                # matching Bot A -- relying on the client library's automatic
                # keepalive here was the original source of the
                # "1011 keepalive ping timeout" crashes.
                async with websockets.connect(KALSHI_WS_URL, ping_interval=None, additional_headers=ws_headers) as ws:
                    print(f"[INFO] {self.bot_name} connected to WebSocket. Locked onto window {target_ticker}.")

                    subscribe_index_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["cfbenchmarks_value"],
                            "index_ids": ["BRTI"]
                        }
                    }
                    await ws.send(json.dumps(subscribe_index_msg))
                    print(f"[INFO] {self.bot_name} subscribed to BTC index feed.")

                    last_ob_check = 0.0
                    ob_data = {"yes_bid_cents": 0, "yes_ask_cents": 100, "no_bid_cents": 0, "no_ask_cents": 100}

                    while True:
                        now_t = time.time()
                        elapsed = now_t - self.window_start_timestamp

                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            data = json.loads(message)
                            msg_type = data.get("type", "unknown")

                            if msg_type == "ping" or message == "heartbeat":
                                await ws.send("pong")
                            elif msg_type == "cfbenchmarks_value":
                                try:
                                    msg_obj = data.get("msg", {})
                                    raw_data_str = msg_obj.get("data", "{}")
                                    parsed_index_data = json.loads(raw_data_str)
                                    self.current_brti = float(parsed_index_data.get("value", 0.0))
                                    if self.current_brti > 0:
                                        self.price_history.append((now_t, self.current_brti))
                                except (json.JSONDecodeError, ValueError, TypeError):
                                    pass
                        except asyncio.TimeoutError:
                            pass
                        except json.JSONDecodeError:
                            pass

                        while self.price_history and self.price_history[0][0] < (now_t - LOOKBACK_SECONDS):
                            self.price_history.popleft()

                        if now_t - last_ob_check > 1.0:
                            fetched_ob = await asyncio.to_thread(self.get_market_orderbook, target_ticker)
                            if fetched_ob:
                                ob_data = fetched_ob
                            last_ob_check = now_t

                        if elapsed >= ACTIVE_PHASE_CUTOFF and self.active_position:
                            side = self.active_position
                            bid = ob_data['yes_bid_cents'] if side == "YES" else ob_data['no_bid_cents']
                            pl = bid - self.entry_price
                            self.recent_losses = self.recent_losses + 1 if pl < 0 else 0
                            exit_msg = f"⏱️ ACTIVE PHASE CUTOFF ({ACTIVE_PHASE_CUTOFF}s): Forced exit {side} @ {bid}c (P/L: {pl:+d}c)"
                            print(f"[INFO] {exit_msg}")
                            self.write_audit_log(exit_msg)
                            self.active_position = None

                        if elapsed >= 900:
                            close_msg = f"🏁 WINDOW EXPIRED | Window: {self.current_window_id} | Total Trades: {self.trade_count}"
                            print(f"[INFO] {close_msg}")
                            self.write_audit_log(close_msg)
                            break

                        recent_prices = [p for _, p in self.price_history]
                        min_p = min(recent_prices) if recent_prices else self.current_brti
                        max_p = max(recent_prices) if recent_prices else self.current_brti
                        upward_spike = (self.current_brti - min_p) if self.current_brti else 0.0
                        downward_spike = (max_p - self.current_brti) if self.current_brti else 0.0

                        # --- ENTRY ---
                        if (self.active_position is None and elapsed < ACTIVE_PHASE_CUTOFF
                                and self.trade_count < MAX_TRADES_PER_WINDOW and self.current_brti):
                            side = None
                            move_val = 0.0
                            if upward_spike >= MIN_BTC_MOVE_USD:
                                side, move_val = "YES", upward_spike
                            elif downward_spike >= MIN_BTC_MOVE_USD:
                                side, move_val = "NO", downward_spike

                            if side:
                                ask = ob_data['yes_ask_cents'] if side == "YES" else ob_data['no_ask_cents']
                                if (MIN_ENTRY_PRICE_CENTS <= ask <= MAX_ENTRY_PRICE_CENTS
                                        and self.is_trading_allowed(self.current_strike, move_val, MIN_BTC_MOVE_USD)):
                                    self.active_position = side
                                    self.entry_price = ask
                                    self.target_sell = ask + PROFIT_TARGET_CENTS
                                    self.stop_price = max(1, ask - STOP_LOSS_CENTS)
                                    self.position_start_time = now_t
                                    self.trade_count += 1

                                    trade_msg = (
                                        f"🎯 ENTRY #{self.trade_count}: Bought {side} @ {ask}c | "
                                        f"Rolling {LOOKBACK_SECONDS}s Move: ${move_val:+.2f} | "
                                        f"Target: {self.target_sell}c | Stop: {self.stop_price}c"
                                    )
                                    print(f"[INFO] {trade_msg}")
                                    self.write_audit_log(trade_msg)

                        # --- POSITION MANAGEMENT ---
                        if self.active_position:
                            side = self.active_position
                            bid = ob_data['yes_bid_cents'] if side == "YES" else ob_data['no_bid_cents']
                            hold_time = now_t - self.position_start_time

                            if bid >= self.target_sell:
                                pl = bid - self.entry_price
                                self.recent_losses = 0
                                exit_msg = f"✅ TAKE PROFIT: Sold {side} @ {bid}c (Gain: +{pl}c)"
                                print(f"[INFO] {exit_msg}")
                                self.write_audit_log(exit_msg)
                                self.active_position = None

                            elif bid <= self.stop_price and bid > 0:
                                pl = bid - self.entry_price
                                self.recent_losses += 1
                                exit_msg = f"🛑 STOP LOSS: Sold {side} @ {bid}c (Loss: {pl}c)"
                                print(f"[INFO] {exit_msg}")
                                self.write_audit_log(exit_msg)
                                self.active_position = None

                            elif hold_time >= HOLD_EXPIRATION_SECONDS:
                                pl = bid - self.entry_price
                                self.recent_losses = self.recent_losses + 1 if pl < 0 else 0
                                exit_msg = f"⏱️ HOLD EXPIRED ({HOLD_EXPIRATION_SECONDS}s): Closed {side} @ {bid}c (P/L: {pl:+d}c)"
                                print(f"[INFO] {exit_msg}")
                                self.write_audit_log(exit_msg)
                                self.active_position = None

            except Exception as e:
                err_msg = f"Exception: {e}. Reconnecting in 5s..."
                print(f"[ERROR] {self.bot_name} {err_msg}")
                self.write_audit_log(f"ERROR: {err_msg}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    bot = BotB()
    asyncio.run(bot.run())
