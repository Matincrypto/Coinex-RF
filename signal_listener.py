import requests
import sqlite3
import time
from datetime import datetime, timezone

# --- Configurations ---
API_URL = "http://103.75.198.172:8080/signals"
DB_NAME = "signals.db"
POLL_INTERVAL = 10  # Check for new signals every 10 seconds

# --- Helper Function for Logging (Added Here) ---
def log(message):
    """Prints a message with a standard UTC timestamp."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

def setup_database():
    """
    Creates the database and the 'signals' table if they don't exist.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_timestamp ON signals (symbol, timestamp)
    ''')
    conn.commit()
    conn.close()
    log("✅ Database is ready.")

def fetch_and_store_signals():
    """
    Fetches signals from the API and stores new ones in the database.
    """
    try:
        response = requests.get(API_URL, timeout=5)
        
        if response.status_code == 200:
            signals_data = response.json()
            if not signals_data:
                log("No new signals at this time.")
                return

            log(f"Received {len(signals_data)} signals from API. Checking for new ones...")
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            
            for signal in signals_data:
                try:
                    symbol = signal['symbol']
                    side = signal['signal_type']
                    price = signal['price']
                    
                    date_string = signal['signal_time_utc']
                    dt_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    timestamp = int(dt_object.timestamp())

                    cursor.execute(
                        'INSERT INTO signals (symbol, side, price, timestamp, status) VALUES (?, ?, ?, ?, ?)',
                        (symbol, side, price, timestamp, 'new')
                    )
                    conn.commit()
                    log(f"  -> 🎉 New signal saved: {symbol} {side} at {price} (Timestamp: {timestamp})")
                except sqlite3.IntegrityError:
                    pass 
                except (KeyError, ValueError) as e:
                    log(f"  -> ⚠️  Warning: Received a signal with unexpected format. Skipping. Data: {signal}, Error: {e}")

            conn.close()
        else:
            log(f"⚠️  API request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        log(f"❌ Network Error: Could not connect to API. Details: {e}")
    except Exception as e:
        log(f"❌ An unexpected error occurred: {e}")

# --- Main part of the script ---
if __name__ == "__main__":
    setup_database()
    
    log("\n🚀 Starting the signal listener... Press Ctrl+C to stop.")
    
    while True:
        log(f"\n--- Checking for signals ---")
        fetch_and_store_signals()
        log(f"Waiting for {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)
