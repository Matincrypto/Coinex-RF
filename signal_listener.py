import requests
import sqlite3
import time

# --- Configurations ---
API_URL = "http://103.75.198.172:8080/signals"
DB_NAME = "signals.db"
POLL_INTERVAL = 10  # Check for new signals every 10 seconds

def setup_database():
    """
    Creates the database and the 'signals' table if they don't exist.
    This function runs only once at the start.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Create table with a 'status' column to track signal state
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
    # Unique index ensures we don't save the exact same signal (symbol + timestamp) twice
    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_timestamp ON signals (symbol, timestamp)
    ''')
    conn.commit()
    conn.close()
    print("✅ Database is ready.")

def fetch_and_store_signals():
    """
    Fetches signals from the API and stores new ones in the database.
    """
    try:
        response = requests.get(API_URL, timeout=5)
        
        if response.status_code == 200:
            signals_data = response.json()
            if not signals_data:
                print("No new signals at this time.")
                return

            print(f"Received {len(signals_data)} signals from API. Checking for new ones...")
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            
            for signal in signals_data:
                try:
                    # اتصال صحیح فیلدهای API به ستون‌های دیتابیس
                    symbol = signal['symbol']
                    side = signal['signal_type']  # اتصال 'signal_type' به 'side'
                    price = signal['price']
                    timestamp = signal['signal_time_utc'] # اتصال 'signal_time_utc' به 'timestamp'

                    # درج سیگنال با وضعیت اولیه 'new'
                    cursor.execute(
                        'INSERT INTO signals (symbol, side, price, timestamp, status) VALUES (?, ?, ?, ?, ?)',
                        (symbol, side, price, timestamp, 'new')
                    )
                    conn.commit()
                    print(f"  -> 🎉 سیگنال جدید ذخیره شد: {symbol} {side} at {price}")
                except sqlite3.IntegrityError:
                    # این اتفاق زمانی می‌افتد که سیگنال تکراری باشد، پس نادیده گرفته می‌شود
                    pass 
                except KeyError:
                    # این هشدار فقط زمانی نمایش داده می‌شود که فیلدهای اصلی واقعاً وجود نداشته باشند
                    print(f"  -> ⚠️  هشدار: سیگنال با فرمت ناشناخته دریافت شد. رد شدن از سیگنال. داده‌ها: {signal}")
                    print(f"  -> ⚠️  Warning: Received a signal with unexpected format. Skipping. Data: {signal}")

            conn.close()
        else:
            print(f"⚠️  API request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Network Error: Could not connect to API. Details: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

# --- Main part of the script ---
if __name__ == "__main__":
    # 1. Prepare the database once at the start
    setup_database()
    
    print("\n🚀 Starting the signal listener... Press Ctrl+C to stop.")
    
    # 2. Start the infinite loop to continuously check for signals
    while True:
        print(f"\n--- [{time.strftime('%Y-%m-%d %H:%M:%S')}] ---")
        fetch_and_store_signals()
        print(f"Waiting for {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)