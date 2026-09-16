import os
import asyncio
import json
import threading
import http.server
import socketserver
import websockets
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
HELIUS_KEY = os.getenv("HELIUS_KEY")
PORT = int(os.getenv("PORT", 10000))

# 1. Light Web Server for Render
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive")
    def log_message(self, format, *args):
        return

def start_server():
    with socketserver.TCPServer(("", PORT), HealthHandler) as httpd:
        httpd.serve_forever()

# 2. Database Logger
def log_to_db(sig):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO executed_trades (signature, status) VALUES (%s, %s) ON CONFLICT (signature) DO NOTHING;",
            (sig, "ACTIVE")
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[DB] Logged transaction: {sig[:10]}...")
    except Exception as e:
        print(f"[DB ERROR] {e}")

# 3. Helius WebSocket Stream
async def run_helius():
    uri = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
    while True:
        try:
            print("[HELIUS] Connecting to Solana stream...")
            async with websockets.connect(uri) as websocket:
                print("[HELIUS] Connected! Listening for blocks...")
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "transactionSubscribe",
                    "params": [
                        {"accountInclude": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]},
                        {"commitment": "processed", "encoding": "jsonParsed"}
                    ]
                }
                await websocket.send(json.dumps(payload))
                
                async for message in websocket:
                    data = json.loads(message)
                    sig = data.get("params", {}).get("result", {}).get("signature")
                    if sig:
                        print(f"[STREAM] Tx: {sig[:10]}")
                        log_to_db(sig)
        except Exception as e:
            print(f"[HELIUS ERROR] {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)

if __name__ == "__main__":
    print("[INIT] Launching background web server...")
    threading.Thread(target=start_server, daemon=True).start()
    
    print("[INIT] Starting Helius async loop...")
    asyncio.run(run_helius())
