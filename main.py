import os, asyncio, json, threading, psycopg2, websockets
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Agent Live")
    def log_message(self, format, *args): return

def init_db():
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS logs (id SERIAL PRIMARY KEY, tx TEXT);")
        conn.commit()
        conn.close()
        print("[NEON DB] Database connected successfully!")
    except Exception as e:
        print("[NEON ERROR]", e)

async def main():
    init_db()
    helius_key = os.getenv("HELIUS_KEY")
    uri = f"wss://mainnet.helius-rpc.com/?api-key={helius_key}"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print("[HELIUS] Websocket Connected!")
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params": [{"mentions": ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"]}, {"commitment": "confirmed"}]
                }))
                while True:
                    msg = await ws.recv()
                    print("[SOLANA TRANSACTION]", msg[:100])
        except Exception as e:
            print("[HELIUS ERROR]", e)
            await asyncio.sleep(5)

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), HealthHandler).serve_forever(), daemon=True).start()
asyncio.run(main())
