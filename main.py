import os
import asyncio
import json
import websockets
import psycopg2
from psycopg2 import OperationalError

# Load environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
HELIUS_KEY = os.getenv("HELIUS_KEY")

def get_db_connection():
    """Establish secure connection to Neon database with error handling"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except OperationalError as e:
        print(f"[NEON DB ERROR] Connection failed: {e}")
        return None

def init_db():
    """Initialize database tables"""
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
    """Log processed transaction into database"""
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

async def listen_helius():
    """Maintain stable Helius WebSocket connection with automatic reconnection"""
    uri = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
    
    while True:
        try:
            print(f"[HELIUS] Connecting to WebSocket...")
            async with websockets.connect(uri) as websocket:
                print("[HELIUS] WebSocket Connected Successfully!")
                
                # Subscription request to monitor transactions
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
    
    try:
        asyncio.run(listen_helius())
    except KeyboardInterrupt:
        print("[BOT] Terminated by user.")
