import os
import time
import requests
import json
import base64
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

KEY_ID_FILE = "key_id.txt"
PEM_FILE = "kalshi_key.pem"
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

with open(KEY_ID_FILE, "r") as f:
    key_id = f.read().strip()
with open(PEM_FILE, "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())

def get_headers(method, path):
    ts = str(int(time.time() * 1000))
    msg = f"{ts}{method}{path.split('?')[0]}".encode('utf-8')
    sig = private_key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": key_id, "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode('utf-8'), "KALSHI-ACCESS-TIMESTAMP": ts, "Content-Type": "application/json"}

# 1. Get current active 15M BTC market
path_markets = "/trade-api/v2/markets?limit=5&status=open&series_ticker=KXBTC15M"
res = requests.get(f"{BASE_URL}/markets?limit=5&status=open&series_ticker=KXBTC15M", headers=get_headers("GET", path_markets))
markets = res.json().get("markets", [])

if markets:
    ticker = sorted(markets, key=lambda x: x.get("close_time", ""))[0]["ticker"]
    print(f"\n==================================================")
    print(f"ACTIVE TICKER: {ticker}")
    print(f"==================================================\n")
    
    # 2. Query Raw Orderbook
    path_ob = f"/trade-api/v2/markets/{ticker}/orderbook"
    ob_res = requests.get(f"{BASE_URL}/markets/{ticker}/orderbook", headers=get_headers("GET", path_ob))
    
    print("--- RAW API RESPONSE PAYLOAD ---")
    print(json.dumps(ob_res.json(), indent=2))
else:
    print("No open markets found.")