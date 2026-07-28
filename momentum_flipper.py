import os
import re
import base64
import time
import requests
import json
import asyncio
import websockets
import logging
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

# --- CONFIGURATION ---
KEY_ID_FILE = "key_id.txt"
PEM_FILE = "kalshi_key.pem"
LOG_FILE = "flipper_audit.log"
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

# --- STRATEGY PARAMETERS & GUARDRAILS ---
DRY_RUN = True                  # True = Paper Trading | False = Live Execution
MAX_POSITION_SIZE = 1           # Contracts per trade
MAX_TRADES_PER_WINDOW = 1       # Prevent overtrading chop by limiting attempts per window
MIN_ENTRY_PRICE_CENTS = 44      # MIN ASK: Ignore cheap/out-of-the-money contracts (< 44¢)
MAX_ENTRY_PRICE_CENTS = 56      # MAX ASK: Ignore over-extended contracts (> 56¢)
PROFIT_TARGET_CENTS = 12        # Exit target (+12¢ gain)
STOP_LOSS_CENTS = 8             # Cut loss (-8¢ drop)
WINDOW_ACTIVE_SECONDS = 180     # Allow entries ONLY during first 180 seconds (3 mins)
MAX_HOLD_SECONDS = 90           # Max hold time per individual trade
MIN_BTC_MOVE_USD = 12.0         # Minimum BTC move ($12.00) required to trigger entry

# Setup dedicated file logging
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

logger = logging.getLogger("MomentumFlipper")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

def log_to_flipper_audit(msg):
    logger.info(msg)
    file_handler.flush()

def extract_strike_price(market_obj):
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

class KalshiEngine:
    def __init__(self):
        self.key_id = None
        self.private_key = None
        self.load_credentials()

    def load_credentials(self):
        if not os.path.exists(KEY_ID_FILE) or not os.path.exists(PEM_FILE):
            raise FileNotFoundError("Missing key_id.txt or kalshi_key.pem in this folder.")

        with open(KEY_ID_FILE, "r") as f:
            self.key_id = f.read().strip()

        with open(PEM_FILE, "rb") as key_file:
            self.private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

    def sign_request(self, timestamp_ms, method, path):
        path_clean = path.split('?')[0]
        message = f"{timestamp_ms}{method}{path_clean}".encode('utf-8')
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    def get_headers(self, method, path):
        timestamp_ms = str(int(time.time() * 1000))
        sig = self.sign_request(timestamp_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "Content-Type": "application/json"
        }

    def get_ws_headers(self):
        return self.get_headers("GET", "/trade-api/ws/v2")

    def get_balance(self):
        path = "/trade-api/v2/portfolio/balance"
        headers = self.get_headers("GET", path)
        res = requests.get(f"{BASE_URL}/portfolio/balance", headers=headers)
        if res.status_code == 200:
            return res.json().get("balance", 0) / 100.0
        return None

    def get_15m_btc_markets(self):
        path = "/trade-api/v2/markets?limit=10&status=open&series_ticker=KXBTC15M"
        headers = self.get_headers("GET", path)
        res = requests.get(f"{BASE_URL}/markets?limit=10&status=open&series_ticker=KXBTC15M", headers=headers)
        if res.status_code == 200:
            markets = res.json().get("markets", [])
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            active_markets = [m for m in markets if m.get("close_time", "") > now_iso]
            return active_markets
        return []

    def get_market_orderbook(self, ticker):
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        headers = self.get_headers("GET", path)
        res = requests.get(f"{BASE_URL}/markets/{ticker}/orderbook", headers=headers)
        
        if res.status_code == 200:
            data = res.json()
            ob = data.get("orderbook_fp", data.get("orderbook", {}))
            
            yes_list = ob.get("yes_dollars") or ob.get("yes") or []
            no_list = ob.get("no_dollars") or ob.get("no") or []
            
            yes_bids = [float(x[0]) for x in yes_list if x and len(x) > 0]
            no_bids = [float(x[0]) for x in no_list if x and len(x) > 0]

            best_yes_bid = max(yes_bids) if yes_bids else 0.0
            best_no_bid = max(no_bids) if no_bids else 0.0

            # Buying YES requires crossing NO's resting bid (1.00 - best_no_bid)
            yes_ask = (1.0 - best_no_bid) if best_no_bid > 0 else 1.00
            no_ask = (1.0 - best_yes_bid) if best_yes_bid > 0 else 1.00

            return {
                "yes_bid_cents": int(round(best_yes_bid * 100)),
                "yes_ask_cents": int(round(yes_ask * 100)),
                "no_bid_cents": int(round(best_no_bid * 100)),
                "no_ask_cents": int(round(no_ask * 100))
            }
        return None

async def run_flipper():
    log_to_flipper_audit(f"ENGINE STARTED | Mode: {'DRY RUN' if DRY_RUN else 'LIVE'} | Range: {MIN_ENTRY_PRICE_CENTS}-{MAX_ENTRY_PRICE_CENTS}c | BTC Move: ${MIN_BTC_MOVE_USD}")

    while True:
        engine = KalshiEngine()
        balance = engine.get_balance()
        
        target_market = None
        while not target_market:
            markets = engine.get_15m_btc_markets()
            if markets:
                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                valid_markets = [m for m in markets if m.get("close_time", "") > now_iso]
                if valid_markets:
                    valid_markets.sort(key=lambda x: x.get("close_time", ""))
                    target_market = valid_markets[0]
            if not target_market:
                await asyncio.sleep(1)

        ticker = target_market.get("ticker")
        close_time_str = target_market.get("close_time")
        strike_price = extract_strike_price(target_market)

        close_dt = datetime.strptime(close_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        window_start_ts = close_dt.timestamp() - 900

        os.system('cls' if os.name == 'nt' else 'clear')
        print("=" * 65)
        print("🚀 MOMENTUM FAST-FLIPPER ENGINE")
        print(f"📝 Audit Log: {LOG_FILE}")
        if balance is not None:
            print(f"💰 Balance Verified: ${balance:,.2f}")
        print("=" * 65)

        log_msg = f"\nMONITORING WINDOW: [{ticker}] | Strike: ${strike_price:,.2f} | Closes: {close_time_str}"
        print(log_msg)
        log_to_flipper_audit(log_msg)

        ws_headers = engine.get_ws_headers()
        in_position = False
        position_data = {}
        total_window_pl = 0
        trade_count = 0
        logged_window_end = False

        try:
            async with websockets.connect(KALSHI_WS_URL, additional_headers=ws_headers) as ws:
                subscribe_msg = {
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {"channels": ["cfbenchmarks_value"], "index_ids": ["all"]}
                }
                await ws.send(json.dumps(subscribe_msg))
                
                last_ob_check = 0
                initial_brti = None
                ob_data = {"yes_bid_cents": 0, "yes_ask_cents": 100, "no_bid_cents": 0, "no_ask_cents": 100}

                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") == "cfbenchmarks_value":
                        body = data.get("msg", {})
                        index_id = body.get("index_id") or data.get("index_id")
                        if not index_id and "data" in body:
                            try:
                                inner_data = json.loads(body["data"]) if isinstance(body["data"], str) else body["data"]
                                index_id = inner_data.get("id")
                            except Exception:
                                pass

                        if index_id != "BRTI":
                            continue

                        brti_raw = body.get("value")
                        if brti_raw is None and "data" in body:
                            try:
                                inner_data = json.loads(body["data"]) if isinstance(body["data"], str) else body["data"]
                                brti_raw = inner_data.get("value")
                            except Exception:
                                pass
                        brti = float(brti_raw) if brti_raw is not None else None

                        if brti and initial_brti is None:
                            initial_brti = brti

                        now_time = time.time()
                        elapsed = now_time - window_start_ts

                        # NON-BLOCKING ORDER BOOK FETCH
                        if now_time - last_ob_check > 1.0:
                            try:
                                fetched_ob = await asyncio.to_thread(engine.get_market_orderbook, ticker)
                                if fetched_ob:
                                    ob_data = fetched_ob
                            except Exception:
                                pass
                            last_ob_check = now_time

                        # LOG 3-MINUTE ENTRY PHASE CONCLUSION ONCE
                        if elapsed >= WINDOW_ACTIVE_SECONDS and not logged_window_end:
                            if in_position:
                                side = position_data["side"]
                                val = ob_data['yes_bid_cents'] if side == "YES" else ob_data['no_bid_cents']
                                pl = val - position_data['entry_price']
                                total_window_pl += pl
                                exit_msg = f"\n⏱️ 3-MIN WINDOW CUTOFF: Forced exit {side} @ {val}c (P/L: {pl:+d}c)"
                                print(exit_msg)
                                log_to_flipper_audit(exit_msg)
                                in_position = False

                            summary_msg = f"\n🏁 ENTRY PHASE CLOSED (180s) | Trades: {trade_count} | Net P/L: {total_window_pl:+d}c\n"
                            print(summary_msg)
                            log_to_flipper_audit(summary_msg)
                            logged_window_end = True

                        # STRICT 15M WINDOW EXPIRATION ROLLOVER
                        if elapsed >= 900:
                            print("\n⏳ 15M Window Expired. Rotating to next market...\n")
                            await asyncio.sleep(2)
                            break

                        # STAGE 1: ENTRY SIGNAL
                        # Guardrail: Check that trade_count hasn't hit MAX_TRADES_PER_WINDOW
                        if not in_position and elapsed < WINDOW_ACTIVE_SECONDS and trade_count < MAX_TRADES_PER_WINDOW and brti and initial_brti:
                            price_delta = brti - initial_brti
                            
                            if abs(price_delta) >= MIN_BTC_MOVE_USD:
                                target_side = "YES" if price_delta > 0 else "NO"
                                target_ask = ob_data['yes_ask_cents'] if target_side == "YES" else ob_data['no_ask_cents']

                                if MIN_ENTRY_PRICE_CENTS <= target_ask <= MAX_ENTRY_PRICE_CENTS:
                                    in_position = True
                                    trade_count += 1
                                    position_data = {
                                        "side": target_side,
                                        "entry_price": target_ask,
                                        "target_sell": target_ask + PROFIT_TARGET_CENTS,
                                        "stop_price": max(1, target_ask - STOP_LOSS_CENTS),
                                        "entry_time": now_time
                                    }
                                    entry_msg = (
                                        f"\n🎯 FLIPPER ENTRY #{trade_count}: Bought {target_side} @ {target_ask}c "
                                        f"(BTC Move: ${price_delta:+.2f} | Target: {position_data['target_sell']}c | Stop: {position_data['stop_price']}c)"
                                    )
                                    print(entry_msg)
                                    log_to_flipper_audit(entry_msg)

                        # STAGE 2: POSITION MANAGEMENT
                        if in_position:
                            side = position_data["side"]
                            current_bid = ob_data['yes_bid_cents'] if side == "YES" else ob_data['no_bid_cents']
                            hold_time = now_time - position_data["entry_time"]

                            if current_bid >= position_data["target_sell"]:
                                pl = current_bid - position_data['entry_price']
                                total_window_pl += pl
                                exit_msg = f"✅ TAKE PROFIT WIN: Sold {side} @ {current_bid}c (Gain: +{pl}c | Running Window P/L: {total_window_pl:+d}c)"
                                print(f"\n{exit_msg}\n")
                                log_to_flipper_audit(exit_msg)
                                in_position = False
                                initial_brti = brti

                            elif current_bid <= position_data["stop_price"] and current_bid > 0:
                                pl = current_bid - position_data['entry_price']
                                total_window_pl += pl
                                exit_msg = f"🛑 STOP LOSS CUT: Sold {side} @ {current_bid}c (Loss: {pl}c | Running Window P/L: {total_window_pl:+d}c)"
                                print(f"\n{exit_msg}\n")
                                log_to_flipper_audit(exit_msg)
                                in_position = False
                                initial_brti = brti

                            elif hold_time >= MAX_HOLD_SECONDS:
                                pl = current_bid - position_data['entry_price']
                                total_window_pl += pl
                                exit_msg = f"⏱️ TRADE HOLD TIME EXPIRED ({MAX_HOLD_SECONDS}s): Closed {side} @ {current_bid}c (P/L: {pl:+d}c)"
                                print(f"\n{exit_msg}\n")
                                log_to_flipper_audit(exit_msg)
                                in_position = False
                                initial_brti = brti

                        # COMPACT SINGLE-LINE TELEMETRY
                        ts = datetime.now().strftime("%H:%M:%S")
                        brti_str = f"${brti:,.2f}" if brti else "Conn..."
                        
                        if in_position:
                            status_tag = f"HOLD {position_data.get('side', '')}@{position_data.get('entry_price', 0)}c"
                        elif trade_count >= MAX_TRADES_PER_WINDOW:
                            status_tag = "DONE (MAX TRADES)"
                        elif elapsed < WINDOW_ACTIVE_SECONDS:
                            status_tag = f"SEARCH ({trade_count}/{MAX_TRADES_PER_WINDOW})"
                        else:
                            status_tag = "MONITOR"

                        out_line = (
                            f"[{ts}] BTC:{brti_str} | {int(elapsed)}s/900s | "
                            f"{status_tag} | Y:{ob_data['yes_ask_cents']}c N:{ob_data['no_ask_cents']}c"
                        )
                        print(f"\r{out_line}\033[K", end="", flush=True)

        except Exception as e:
            err_msg = f"Flipper Exception: {e}. Reconnecting in 3s..."
            print(f"\n{err_msg}")
            log_to_flipper_audit(err_msg)

        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(run_flipper())
    except KeyboardInterrupt:
        print("\n\nFlipper stopped.")