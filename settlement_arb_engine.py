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

# Windows console emoji-safety (see flipper_variant_a.py).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# --- CONFIGURATION ---
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_REST_URL = "https://api.elections.kalshi.com/trade-api/v2"

# --- MEASUREMENT PARAMETERS ---
# This engine NEVER trades. It records, at fixed checkpoints before each 15-min
# settlement, what the near-certain winner is and what price it is buyable at,
# plus the official post-settlement result. A few days of this tells us whether
# the "buy the near-decided winner at a discount" convergence edge is real
# before we commit to building a trading bot around it.
# Cover the full late-window convergence curve, not just the final minute. The
# first live window showed the winner already fully priced (~99c) by T-90s, but
# only ~73c at T-350s while already a strong favorite -- so any real edge lives
# in the 3-6 min pre-close zone, and the measurement has to reach out that far.
CHECKPOINTS_SEC = [300, 240, 180, 120, 90, 60, 30, 15]  # seconds-before-close to log an observation
CONFIDENCE_MARGIN = 10.0   # |avg_60s - strike| >= this (USD) => "high confidence" winner
MAX_BUY_PRICE_CENTS = 85   # a winner buyable <= this is tagged as an actionable edge
MIN_AVG_WINDOW = 45        # trust avg_60s only once its rolling buffer has >= this many samples
STATUS_UPDATE_INTERVAL = 20

load_dotenv()
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")

def load_private_key(file_path):
    with open(file_path, "rb") as key_file:
        return serialization.load_pem_private_key(key_file.read(), password=None)

def extract_strike_price(market_obj):
    strike = market_obj.get("floor_strike") or market_obj.get("cap_strike")
    if strike and float(strike) > 0:
        return float(strike)
    text = f"{market_obj.get('title', '')} {market_obj.get('subtitle', '')}"
    matches = re.findall(r"\$?([0-9]{2,3},?[0-9]{3}\.?[0-9]*)", text)
    if matches:
        try:
            return float(matches[0].replace(",", ""))
        except ValueError:
            pass
    return 0.0

class SettlementMonitor:
    def __init__(self):
        self.bot_name = "SETTLEMENT MONITOR"
        self.private_key = load_private_key(KALSHI_PRIVATE_KEY_PATH)
        self.current_window_id = ""
        self.close_timestamp = 0.0
        self.strike = 0.0
        self.current_brti = 0.0
        self.avg_60s = 0.0
        self.avg_window_size = 0
        self.logged_checkpoints = set()
        self.last_status_time = 0.0

    def write_audit_log(self, message):
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        with open(f"{self.bot_name.replace(' ', '_')}_audit.log", "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")

    def sign(self, method, path):
        ts = str(int(time.time() * 1000))
        msg = f"{ts}{method}{path.split('?')[0]}".encode("utf-8")
        sig = self.private_key.sign(
            msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    def get_live_market(self):
        """Returns (ticker, close_timestamp, strike) for the open KXBTC15M market closing soonest."""
        headers = self.sign("GET", "/trade-api/v2/markets")
        try:
            res = requests.get(f"{KALSHI_REST_URL}/markets?limit=5&status=open&series_ticker=KXBTC15M",
                               headers=headers, timeout=10)
            if res.status_code != 200:
                return None
            markets = res.json().get("markets", [])
            if not markets:
                return None
            markets.sort(key=lambda m: m.get("close_time", ""))
            m = markets[0]
            close_dt = datetime.strptime(m["close_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return m["ticker"], close_dt.timestamp(), extract_strike_price(m)
        except (requests.RequestException, KeyError, ValueError):
            return None

    def get_official_result(self, ticker):
        """Post-close: read the settled market's result ('yes'/'no'/'')."""
        headers = self.sign("GET", f"/trade-api/v2/markets/{ticker}")
        try:
            res = requests.get(f"{KALSHI_REST_URL}/markets/{ticker}", headers=headers, timeout=10)
            if res.status_code == 200:
                return res.json().get("market", {}).get("result", "")
        except requests.RequestException:
            pass
        return ""

    async def run(self):
        startup = f"ENGINE STARTED [{self.bot_name}] | MEASUREMENT ONLY (never trades) | checkpoints {CHECKPOINTS_SEC}s pre-close"
        print(f"[INFO] {startup}")
        self.write_audit_log(startup)

        while True:
            try:
                live = await asyncio.to_thread(self.get_live_market)
                if not live:
                    await asyncio.sleep(2)
                    continue
                target_ticker, close_ts, strike = live
                if time.time() >= close_ts:  # already closed listing lag
                    await asyncio.sleep(2)
                    continue

                if target_ticker != self.current_window_id:
                    self.current_window_id = target_ticker
                    self.close_timestamp = close_ts
                    self.strike = strike
                    self.logged_checkpoints = set()
                    self.avg_window_size = 0

                ws_headers = self.sign("GET", "/trade-api/ws/v2")
                async with websockets.connect(KALSHI_WS_URL, ping_interval=None, additional_headers=ws_headers) as ws:
                    print(f"[INFO] Locked window {target_ticker} | strike ${strike:,.2f} | closes in {int(close_ts - time.time())}s")
                    await ws.send(json.dumps({"id": 1, "cmd": "subscribe",
                        "params": {"channels": ["ticker"], "market_tickers": [target_ticker]}}))
                    await ws.send(json.dumps({"id": 2, "cmd": "subscribe",
                        "params": {"channels": ["cfbenchmarks_value"], "index_ids": ["BRTI"]}}))

                    yes_bid = yes_ask = no_bid = no_ask = 0
                    yes_bid_size = yes_ask_size = 0.0

                    async for message in ws:
                        data = json.loads(message)
                        mtype = data.get("type", "")
                        if mtype == "ping" or message == "heartbeat":
                            await ws.send("pong")
                            continue

                        if mtype == "cfbenchmarks_value":
                            try:
                                m = data.get("msg", {})
                                self.current_brti = float(json.loads(m.get("data", "{}")).get("value", 0.0))
                                avg = m.get("avg_60s_data", {})
                                if avg:
                                    self.avg_60s = float(avg.get("value", 0.0))
                                    self.avg_window_size = int(avg.get("window_size", 0))
                            except (json.JSONDecodeError, ValueError, TypeError):
                                pass

                        elif mtype == "ticker":
                            m = data.get("msg", {})
                            if m.get("market_ticker", "") != self.current_window_id:
                                continue
                            yes_bid = int(float(m.get("yes_bid_dollars", "0.0")) * 100)
                            yes_ask = int(float(m.get("yes_ask_dollars", "0.0")) * 100)
                            no_bid = 100 - yes_ask if yes_ask > 0 else 0
                            no_ask = 100 - yes_bid if yes_bid > 0 else 100
                            yes_bid_size = float(m.get("yes_bid_size_fp", "0") or 0)
                            yes_ask_size = float(m.get("yes_ask_size_fp", "0") or 0)

                        ttc = self.close_timestamp - time.time()

                        if time.time() - self.last_status_time >= STATUS_UPDATE_INTERVAL and ttc > 0:
                            margin = self.avg_60s - self.strike if self.avg_60s else 0.0
                            print(f"[STATUS] {self.current_window_id} | close in {int(ttc)}s | "
                                  f"avg60 ${self.avg_60s:,.2f} (n={self.avg_window_size}) | strike ${self.strike:,.2f} | "
                                  f"margin ${margin:+.2f} | Y_ask {yes_ask}c N_ask {no_ask}c")
                            self.last_status_time = time.time()

                        # --- record an observation as we cross each checkpoint ---
                        for cp in CHECKPOINTS_SEC:
                            if cp not in self.logged_checkpoints and ttc <= cp and self.avg_60s > 0:
                                self.logged_checkpoints.add(cp)
                                margin = self.avg_60s - self.strike
                                winner = "YES" if margin > 0 else "NO"
                                winner_ask = yes_ask if winner == "YES" else no_ask
                                # Depth available to BUY the winner: take the YES ask for a
                                # YES winner, or cross the YES bid (= the synthetic NO ask)
                                # for a NO winner. Tells us whether the buyable price has real
                                # size behind it or is a 1-lot phantom quote -- the one thing
                                # that decides if this edge is deployable.
                                winner_depth = yes_ask_size if winner == "YES" else yes_bid_size
                                conf = "HI" if abs(margin) >= CONFIDENCE_MARGIN else "LO"
                                buyable = "Y" if (0 < winner_ask <= MAX_BUY_PRICE_CENTS) else "N"
                                rec = (f"SETTLE-OBS | {self.current_window_id} | T-{cp}s | "
                                       f"avg={self.avg_60s:.2f} | strike={self.strike:.2f} | margin={margin:+.2f} | "
                                       f"winner={winner} | winner_ask={winner_ask}c | ask_depth={winner_depth:.0f} | "
                                       f"conf={conf} | buyable={buyable} | wsize={self.avg_window_size}")
                                print(f"[OBS] {rec}")
                                self.write_audit_log(rec)

                        # --- window closed: capture de-facto + official result, then roll ---
                        if ttc <= 0:
                            defacto = "YES" if (self.avg_60s - self.strike) > 0 else "NO"
                            await asyncio.sleep(8)  # let settlement post to REST before reading result
                            official = await asyncio.to_thread(self.get_official_result, self.current_window_id)
                            rec = (f"SETTLE-RESULT | {self.current_window_id} | final_avg={self.avg_60s:.2f} | "
                                   f"strike={self.strike:.2f} | defacto={defacto} | official={official or 'pending'} | "
                                   f"last_Y_ask={yes_ask}c last_N_ask={no_ask}c")
                            print(f"[RESULT] {rec}")
                            self.write_audit_log(rec)
                            break

            except Exception as e:
                err = f"Exception: {e}. Reconnecting in 5s..."
                print(f"[ERROR] {err}")
                self.write_audit_log(f"ERROR: {err}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(SettlementMonitor().run())
