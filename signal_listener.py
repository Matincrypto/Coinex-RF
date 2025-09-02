# signal_listener.py (نسخه نهایی و هماهنگ)

import requests
import sqlite3
import time
from datetime import datetime, timezone

# --- Configurations ---
API_URL = "http://103.75.198.172:8080/signals"
DB_NAME = "signals.db"
POLL_INTERVAL = 10

# --- Helper Function for Logging ---
def log(message):
    """Prints a message with a standard UTC timestamp."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

def setup_database():
    """
    Creates the database and sets up WAL mode for better concurrency.
    The UNIQUE INDEX is removed to allow all signals to be stored.
    """
    try:
        conn = sqlite3.connect(DB_NAME, timeout=15)
        cursor = conn.cursor()
        
        # فعال کردن حالت WAL برای جلوگیری از قفل شدن دیتابیس
        cursor.execute('PRAGMA journal_mode=WAL;')

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
        conn.commit()
        log("✅ Database is ready in WAL mode. All incoming signals will be stored.")
    except Exception as e:
        log(f"❌ CRITICAL ERROR during database setup: {e}")
    finally:
        if conn:
            conn.close()

def fetch_and_store_signals():
    """
    Fetches signals from the API and stores all of them in the database.
    """
    conn = None
    try:
        response = requests.get(API_URL, timeout=10)
        
        if response.status_code == 200:
            signals_data = response.json()
            if not signals_data:
                log("No signals received from API at this time.")
                return

            log(f"Received {len(signals_data)} signals from API. Storing all...")
            
            conn = sqlite3.connect(DB_NAME, timeout=15)
            conn.execute('PRAGMA journal_mode=WAL;')
            cursor = conn.cursor()
            
            new_signals_count = 0
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
                    new_signals_count += 1
                
                except (KeyError, ValueError) as e:
                    log(f"  -> ⚠️  Warning: Received a signal with unexpected format. Skipping. Data: {signal}, Error: {e}")

            conn.commit()
            if new_signals_count > 0:
                log(f"  -> 🎉 Stored {new_signals_count} new signals in the database.")
        else:
            log(f"⚠️  API request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        log(f"❌ Network Error: Could not connect to API. Details: {e}")
    except sqlite3.Error as e:
        log(f"❌ Database Error in listener: {e}")
    except Exception as e:
        log(f"❌ An unexpected error occurred in listener: {e}")
    finally:
        if conn:
            conn.close()

# --- Main part of the script ---
if __name__ == "__main__":
    setup_database()
    log("\n🚀 Starting the signal listener... Press Ctrl+C to stop.")
    
    while True:
        try:
            log(f"\n--- Checking for signals ---")
            fetch_and_store_signals()
            log(f"Waiting for {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log("🛑 User interrupted the listener. Shutting down.")
            break
        except Exception as e:
            log(f"❌ CRITICAL ERROR in listener main loop: {e}")
            time.sleep(POLL_INTERVAL) # Wait before retrying