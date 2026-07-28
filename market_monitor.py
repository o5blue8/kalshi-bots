import asyncio
import json
import time
from datetime import datetime
import websockets

# Coinbase Real-Time WebSocket Endpoint
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

async def monitor_btc():
    print("=" * 60)
    print("🚀 Starting BTC Live Latency Tracker")
    print("Press Ctrl+C to stop the script at any time.")
    print("=" * 60)
    
    async with websockets.connect(COINBASE_WS_URL) as ws:
        # Subscribe to Coinbase BTC-USD ticker feed
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"]
        }
        await ws.send(json.dumps(subscribe_msg))
        
        last_printed_price = None

        async for message in ws:
            data = json.loads(message)
            
            # Filter for ticker updates
            if data.get("type") == "ticker" and "price" in data:
                current_price = float(data["price"])
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                
                # Only print when the price actually moves to reduce terminal clutter
                if current_price != last_printed_price:
                    diff = 0
                    if last_printed_price is not None:
                        diff = current_price - last_printed_price
                    
                    direction = "🟢" if diff > 0 else "🔴" if diff < 0 else "⚪"
                    change_str = f"({diff:+.2f})" if last_printed_price else "(init)"
                    
                    print(f"[{timestamp}] {direction} BTC Spot: ${current_price:,.2f} {change_str}")
                    last_printed_price = current_price

if __name__ == "__main__":
    try:
        asyncio.run(monitor_btc())
    except KeyboardInterrupt:
        print("\nTracker stopped.")