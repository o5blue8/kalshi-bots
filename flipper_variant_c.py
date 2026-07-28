import os, re, base64, time, requests, json, asyncio, websockets, logging
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

KEY_ID_FILE, PEM_FILE = "key_id.txt", "kalshi_key.pem"
LOG_FILE = "audit_c_us_session.log"
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

# PARAMETERS - BOT C (US HOURS & CONDITIONAL RE-ENTRY)
DRY_RUN = True
US_START_HOUR_UTC = 13        # 13:00 UTC = 8:00 AM EST (US Market Prep)
US_END_HOUR_UTC = 21          # 21:00 UTC = 4:00 PM EST (US Close)
MIN_ENTRY_PRICE_CENTS = 44
MAX_ENTRY_PRICE_CENTS = 56
PROFIT_TARGET_CENTS = 12
STOP_LOSS_CENTS = 10          # Balanced -10c stop
WINDOW_ACTIVE_SECONDS = 180
MAX_HOLD_SECONDS = 90
MIN_BTC_MOVE_USD = 15.0

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("BotC")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

def log_audit(msg):
    logger.info(msg)
    file_handler.flush()

def extract_strike_price(market_obj):
    strike = market_obj.get("floor_strike") or market_obj.get("cap_strike")
    if strike and float(strike) > 0: return float(strike)
    text = f"{market_obj.get('title', '')} {market_obj.get('subtitle', '')}"
    matches = re.findall(r"\$?([0-9]{2,3},?[0-9]{3}\.?[0-9]*)", text)
    return float(matches[0].replace(",", "")) if matches else 0.0

def is_us_session():
    now_utc_hour = datetime.now(timezone.utc).hour
    return US_START_HOUR_UTC <= now_utc_hour < US_END_HOUR_UTC

class KalshiEngine:
    def __init__(self):
        self.load_credentials()

    def load_credentials(self):
        with open(KEY_ID_FILE, "r") as f: self.key_id = f.read().strip()
        with open(PEM_FILE, "rb") as kf:
            self.private_key = serialization.load_pem_private_key(kf.read(), password=None, backend=default_backend())

    def get_headers(self, method, path):
        ts = str(int(time.time() * 1000))
        msg = f"{ts}{method}{path.split('?')[0]}".encode('utf-8')
        sig = base64.b64encode(self.private_key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())).decode('utf-8')
        return {"KALSHI-ACCESS-KEY": self.key_id, "KALSHI-ACCESS-SIGNATURE": sig, "KALSHI-ACCESS-TIMESTAMP": ts, "Content-Type": "application/json"}

    def get_15m_btc_markets(self):
        res = requests.get(f"{BASE_URL}/markets?limit=10&status=open&series_ticker=KXBTC15M", headers=self.get_headers("GET", "/trade-api/v2/markets"))
        if res.status_code == 200:
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            return [m for m in res.json().get("markets", []) if m.get("close_time", "") > now_iso]
        return []

    def get_market_orderbook(self, ticker):
        res = requests.get(f"{BASE_URL}/markets/{ticker}/orderbook", headers=self.get_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook"))
        if res.status_code == 200:
            ob = res.json().get("orderbook_fp", res.json().get("orderbook", {}))
            yb = [float(x[0]) for x in (ob.get("yes_dollars") or ob.get("yes") or []) if x]
            nb = [float(x[0]) for x in (ob.get("no_dollars") or ob.get("no") or []) if x]
            best_yb, best_nb = (max(yb) if yb else 0.0), (max(nb) if nb else 0.0)
            return {"yes_bid_cents": int(round(best_yb*100)), "yes_ask_cents": int(round((1.0-best_nb if best_nb>0 else 1.0)*100)),
                    "no_bid_cents": int(round(best_nb*100)), "no_ask_cents": int(round((1.0-best_yb if best_yb>0 else 1.0)*100))}
        return None

async def run_flipper():
    log_audit(f"ENGINE STARTED [BOT C - US HOURS & WIN SCALER] | Hours: {US_START_HOUR_UTC}:00-{US_END_HOUR_UTC}:00 UTC | Move: ${MIN_BTC_MOVE_USD}")
    while True:
        if not is_us_session():
            await asyncio.sleep(30)
            continue

        engine = KalshiEngine()
        target_market = None
        while not target_market:
            m = engine.get_15m_btc_markets()
            if m:
                m.sort(key=lambda x: x.get("close_time", ""))
                target_market = m[0]
            if not target_market: await asyncio.sleep(1)

        ticker = target_market.get("ticker")
        close_time_str = target_market.get("close_time")
        strike = extract_strike_price(target_market)
        close_dt = datetime.strptime(close_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        window_start_ts = close_dt.timestamp() - 900

        log_audit(f"\nMONITORING WINDOW: [{ticker}] | Strike: ${strike:,.2f} | Closes: {close_time_str}")

        in_pos, pos_data, total_pl, trade_cnt, logged_end = False, {}, 0, 0, False
        last_trade_was_win = False  # Allows Trade #2 ONLY if Trade #1 was a WIN
        ws_headers = engine.get_headers("GET", "/trade-api/ws/v2")

        try:
            async with websockets.connect(KALSHI_WS_URL, additional_headers=ws_headers) as ws:
                await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["cfbenchmarks_value"], "index_ids": ["all"]}}))
                last_ob, initial_brti = 0, None
                ob_data = {"yes_bid_cents": 0, "yes_ask_cents": 100, "no_bid_cents": 0, "no_ask_cents": 100}

                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") == "cfbenchmarks_value":
                        body = data.get("msg", {})
                        idx = body.get("index_id") or data.get("index_id")
                        if idx != "BRTI": continue
                        brti = float(body.get("value")) if body.get("value") is not None else None
                        if brti and initial_brti is None: initial_brti = brti

                        now_t = time.time()
                        elapsed = now_t - window_start_ts

                        if now_t - last_ob > 1.0:
                            f_ob = await asyncio.to_thread(engine.get_market_orderbook, ticker)
                            if f_ob: ob_data = f_ob
                            last_ob = now_t

                        if elapsed >= WINDOW_ACTIVE_SECONDS and not logged_end:
                            if in_pos:
                                val = ob_data['yes_bid_cents'] if pos_data["side"] == "YES" else ob_data['no_bid_cents']
                                pl = val - pos_data['entry_price']
                                total_pl += pl
                                log_audit(f"⏱️ 3-MIN CUTOFF: Forced exit {pos_data['side']} @ {val}c (P/L: {pl:+d}c)")
                                in_pos = False
                            log_audit(f"🏁 ENTRY PHASE CLOSED (180s) | Trades: {trade_cnt} | Net P/L: {total_pl:+d}c\n")
                            logged_end = True

                        if elapsed >= 900: break

                        # Entry Condition: Allowed if trade_cnt == 0 OR (trade_cnt == 1 AND last_trade_was_win)
                        can_entry = (trade_cnt == 0) or (trade_cnt == 1 and last_trade_was_win)
                        if not in_pos and elapsed < WINDOW_ACTIVE_SECONDS and can_entry and brti and initial_brti:
                            delta = brti - initial_brti
                            if abs(delta) >= MIN_BTC_MOVE_USD:
                                side = "YES" if delta > 0 else "NO"
                                ask = ob_data['yes_ask_cents'] if side == "YES" else ob_data['no_ask_cents']
                                if MIN_ENTRY_PRICE_CENTS <= ask <= MAX_ENTRY_PRICE_CENTS:
                                    in_pos = True
                                    trade_cnt += 1
                                    pos_data = {"side": side, "entry_price": ask, "target_sell": ask + PROFIT_TARGET_CENTS, "stop_price": max(1, ask - STOP_LOSS_CENTS), "entry_time": now_t}
                                    log_audit(f"🎯 ENTRY #{trade_cnt}: Bought {side} @ {ask}c (Move: ${delta:+.2f} | Target: {pos_data['target_sell']}c | Stop: {pos_data['stop_price']}c)")

                        if in_pos:
                            side = pos_data["side"]
                            bid = ob_data['yes_bid_cents'] if side == "YES" else ob_data['no_bid_cents']
                            hold_t = now_t - pos_data["entry_time"]
                            if bid >= pos_data["target_sell"]:
                                pl = bid - pos_data['entry_price']
                                total_pl += pl
                                last_trade_was_win = True
                                log_audit(f"✅ TAKE PROFIT: Sold {side} @ {bid}c (Gain: +{pl}c | Window P/L: {total_pl:+d}c)")
                                in_pos, initial_brti = False, brti
                            elif bid <= pos_data["stop_price"] and bid > 0:
                                pl = bid - pos_data['entry_price']
                                total_pl += pl
                                last_trade_was_win = False
                                log_audit(f"🛑 STOP LOSS: Sold {side} @ {bid}c (Loss: {pl}c | Window P/L: {total_pl:+d}c)")
                                in_pos, initial_brti = False, brti
                            elif hold_t >= MAX_HOLD_SECONDS:
                                pl = bid - pos_data['entry_price']
                                total_pl += pl
                                last_trade_was_win = (pl > 0)
                                log_audit(f"⏱️ HOLD EXPIRED (90s): Closed {side} @ {bid}c (P/L: {pl:+d}c)")
                                in_pos, initial_brti = False, brti
        except Exception as e:
            log_audit(f"Bot C Exception: {e}. Reconnecting in 3s...")
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(run_flipper())