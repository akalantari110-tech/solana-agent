import os
import asyncio
import json
import threading
import http.server
import socketserver
import websockets
import psycopg2
from psycopg2 import OperationalError

# Load environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
HELIUS_KEY = os.getenv("HELIUS_KEY")
PORT = int(os.getenv("PORT", 10000))

# 1. Dummy HTTP Server for Render Web Service health check
class SimpleHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Solana Trading Agent is running!")

def run_web_server():
    """Run a lightweight web server to satisfy Render's port binding requirement"""
    try:
        with socketserver.TCPServer(("", PORT), SimpleHandler) as httpd:
            print(f"[WEB] Dummy web server started on port {PORT}")
            httpd.serve_forever()
    except Exception as e:
        print(f"[WEB ERROR] Failed to start web server: {e}")

# 2. Database Functions
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except OperationalError as e:
        print(f"[NEON DB ERROR] Connection failed: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS executed_trades (
                    id SERIAL PRIMARY KEY,
                    signature TEXT UNIQUE,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("[NEON DB] Tables initialized successfully.")
        except Exception as e:
            print(f"[NEON DB ERROR] Table creation failed: {e}")

async def log_trade_to_db(signature):
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO executed_trades (signature, status) VALUES (%s, %s) ON CONFLICT (signature) DO NOTHING;",
                (signature, "PROCESSED")
            )
            conn.commit()
            cur.close()
            conn.close()
            print(f"[NEON DB] Transaction {signature[:8]}... logged to database.")
        except Exception as e:
            print(f"[NEON DB ERROR] Insert failed: {e}")

# 3. Helius WebSocket Stream
async def listen_helius():
    uri = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
    
    while True:
        try:
            print(f"[HELIUS] Connecting to WebSocket...")
            async with websockets.connect(uri) as websocket:
                print("[HELIUS] WebSocket Connected Successfully!")
                
                subscribe_request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "transactionSubscribe",
                    "params": [
                        {"accountInclude": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]},
                        {"commitment": "processed", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
                    ]
                }
                
                await websocket.send(json.dumps(subscribe_request))
                print("[HELIUS] Subscribed to transaction stream.")

                async for message in websocket:
                    data = json.loads(message)
                    try:
                        tx_signature = data.get("params", {}).get("result", {}).get("signature")
                        if tx_signature:
                            print(f"[AGENT] New Transaction Captured: {tx_signature}")
                            await log_trade_to_db(tx_signature)
                    except Exception as parse_err:
                        print(f"[PARSER ERROR] Failed to parse message: {parse_err}")
                        
        except websockets.exceptions.ConnectionClosed as e:
            print(f"[HELIUS ERROR] Connection closed ({e}). Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[HELIUS ERROR] Unexpected error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    print("[BOT] Initializing Solana Trading Agent...")
    init_db()
    
    # Start the web server in a separate background thread for Render health check
    server_thread = threading.Thread(target=run_web_server, daemon=True)
    server_thread.start()
    
    # Run the main async loop for Helius
    try:
        asyncio.run(listen_helius())
    except KeyboardInterrupt:
        print("[BOT] Terminated by user.")
