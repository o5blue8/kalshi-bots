import os
import re
import base64
import time
import requests
import json
import asyncio
import websockets
import logging
from datetime import datetime
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

# --- CONFIGURATION ---
KEY_ID_FILE = "key_id.txt"
PEM_FILE = "kalshi_key.pem"
LOG_FILE = "settlement_audit.log"
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

# --- TRADING GUARDRAILS ---
DRY_RUN = True              # True = Paper Trading | False = Live Execution
MAX_POSITION_SIZE = 1       # Contracts per trade
MAX_BUY_PRICE_CENTS = 85    # Buy winning contract ONLY if priced <= 85¢ (Min 15¢ edge)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

logger = logging.getLogger("SettlementEngine")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

def log_to_audit(msg):
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

def get_actual_window_start_time():
    now = time.time()
    return now - (now % 900)

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
            return res.json().get("markets", [])
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
            
            best_yes_bid = float(yes_list[-1][0]) if yes_list else 0.0
            best_no_bid = float(no_list[-1][0]) if no_list else 0.0
            
            # Robust ask derivation
            best_yes_ask = (1.0 - best_no_bid) if best_no_bid > 0 else (float(yes_list[0][0]) if yes_list else 0.0)
            best_no_ask = (1.0 - best_yes_bid) if best_yes_bid > 0 else (float(no_list[0][0]) if no_list else 0.0)
            
            return {
                "yes_ask_cents": int(round(best_yes_ask * 100)),
                "no_ask_cents": int(round(best_no_ask * 100))
            }
        return None

async def run_live_monitor():
    print("=" * 65)
    print("SETTLEMENT ARBITRAGE AUDIT ENGINE")
    print(f"Logging Directly To: {LOG_FILE}")
    print("=" * 65)
    
    log_to_audit(f"ENGINE STARTED | Mode: {'DRY RUN' if DRY_RUN else 'LIVE'} | Max Buy Limit: {MAX_BUY_PRICE_CENTS}c")

    engine = KalshiEngine()
    balance = engine.get_balance()
    print(f"Balance Verified: ${balance:,.2f}\n")

    current_ticker = None

    while True:
        markets = engine.get_15m_btc_markets()
        if not markets:
            print("No open 15M markets found. Retrying in 10s...")
            await asyncio.sleep(10)
            continue

        markets.sort(key=lambda x: x.get("close_time", ""))
        target_market = markets[0]
        ticker = target_market.get("ticker")
        close_time = target_market.get("close_time")
        strike_price = extract_strike_price(target_market)

        if ticker != current_ticker:
            current_ticker = ticker
            log_msg = f"MONITORING WINDOW: [{ticker}] | Strike: ${strike_price:,.2f} | Closes: {close_time}"
            print(f"\n{log_msg}")
            log_to_audit(log_msg)

        ws_headers = engine.get_ws_headers()

        try:
            async with websockets.connect(KALSHI_WS_URL, additional_headers=ws_headers) as ws:
                subscribe_msg = {
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {"channels": ["cfbenchmarks_value"], "index_ids": ["all"]}
                }
                await ws.send(json.dumps(subscribe_msg))
                
                last_ob_check = 0
                ob_data = {"yes_ask_cents": 0, "no_ask_cents": 0}

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

                        avg_obj = body.get("last_60s_windowed_average_15min", {})
                        avg_val = avg_obj.get("value") if isinstance(avg_obj, dict) else None
                        avg_60s = float(avg_val) if avg_val is not None else None

                        now_time = time.time()
                        if now_time - last_ob_check > 2.0:
                            fetched_ob = engine.get_market_orderbook(ticker)
                            if fetched_ob:
                                ob_data = fetched_ob
                            last_ob_check = now_time

                        # EVALUATE ONCE AT SETTLEMENT
                        if avg_60s:
                            yes_ask = ob_data['yes_ask_cents']
                            no_ask = ob_data['no_ask_cents']

                            winning_side = "YES" if (strike_price == 0 or avg_60s > strike_price) else "NO"
                            winning_ask = yes_ask if winning_side == "YES" else no_ask

                            if winning_ask > 0 and winning_ask <= MAX_BUY_PRICE_CENTS:
                                action = f"[SIMULATED BUY {winning_side} @ {winning_ask}c]" if DRY_RUN else f"[EXECUTED BUY {winning_side} @ {winning_ask}c]"
                                reason = f"{winning_side} won (60s Avg ${avg_60s:,.2f} vs Strike ${strike_price:,.2f}) & Ask {winning_ask}c <= {MAX_BUY_PRICE_CENTS}c limit."
                            else:
                                action = "[NO TRADE - NO EDGE]"
                                reason = f"{winning_side} won (60s Avg ${avg_60s:,.2f} vs Strike ${strike_price:,.2f}), but Ask ({winning_ask}c) exceeded limit ({MAX_BUY_PRICE_CENTS}c)."

                            audit_record = (
                                f"\n==================== 15-MIN WINDOW AUDIT ====================\n"
                                f"Contract Ticker : {ticker}\n"
                                f"Strike Baseline : ${strike_price:,.2f}\n"
                                f"Final 60s Avg   : ${avg_60s:,.2f}\n"
                                f"Order Book Asks : YES: {yes_ask}c | NO: {no_ask}c\n"
                                f"Action Taken    : {action}\n"
                                f"Reason          : {reason}\n"
                                f"============================================================="
                            )
                            log_to_audit(audit_record)
                            print(f"\n{audit_record}\n")
                            
                            # SLEEP UNTIL NEXT WINDOW STARTS TO PREVENT REPEATED LOGS
                            window_start = get_actual_window_start_time()
                            remaining_secs = max(1, int(900 - (time.time() - window_start)) + 2)
                            print(f"⏳ Settlement Audited. Sleeping {remaining_secs}s for next window...")
                            await asyncio.sleep(remaining_secs)
                            break

                        ts = datetime.now().strftime("%H:%M:%S")
                        brti_str = f"${brti:,.2f}" if brti else "Connecting..."
                        avg_str = f"${avg_60s:,.2f}" if avg_60s else "Inactive (Settles MM:14-15)"
                        
                        print(
                            f"[{ts}] BTC: {brti_str} | 60s Avg: {avg_str} | "
                            f"YES Ask: {ob_data['yes_ask_cents']}c | NO Ask: {ob_data['no_ask_cents']}c      ",
                            end="\r"
                        )

        except Exception as e:
            err_msg = f"Window Exception: {e}. Reconnecting in 5s..."
            print(f"\n{err_msg}")
            log_to_audit(err_msg)

        await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(run_live_monitor())
    except KeyboardInterrupt:
        print("\n\nEngine stopped.")