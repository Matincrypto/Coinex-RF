# signal_listener.py (نسخه جدید با معماری هوشمند)

import requests
import sqlite3
import time
from datetime import datetime, timezone
from telegram_logger import send_message

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
    Creates or updates the database schema for the new logic.
    - 'signals' table uses 'symbol' as the PRIMARY KEY to store only the latest signal.
    - 'processed_signal_ids' table stores all seen signal IDs to prevent duplicates.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME, timeout=15)
        cursor = conn.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')

        # جدول اصلی سیگنال‌ها: برای هر symbol فقط یک ردیف (آخرین سیگنال) وجود خواهد داشت
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                symbol TEXT PRIMARY KEY,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                timestamp INTEGER NOT NULL,
                status TEXT NOT NULL,
                signal_id TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # جدولی برای نگهداری تمام signal_id های دیده شده برای جلوگیری از پردازش تکراری
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_signal_ids (
                signal_id TEXT PRIMARY KEY
            )
        ''')

        conn.commit()
        log("✅ Database is ready with the new architecture.")
    except Exception as e:
        error_msg = f"<b>❌ CRITICAL ERROR (Listener)</b>\n\nFailed during database setup.\n\n<b>Error:</b>\n<code>{e}</code>"
        log(f"CRITICAL ERROR during database setup: {e}")
        send_message(error_msg)
    finally:
        if conn:
            conn.close()

def fetch_and_store_signals():
    """
    Fetches signals, ignores seen signal_ids, and replaces signals based on symbol.
    """
    conn = None
    try:
        response = requests.get(API_URL, timeout=10)

        if response.status_code == 200:
            signals_data = response.json()
            if not signals_data:
                log("No new signals received from API.")
                return

            # Handle case where API might return a single object instead of a list
            if isinstance(signals_data, dict):
                signals_data = [signals_data]

            log(f"Received {len(signals_data)} signal(s) from API. Processing...")

            conn = sqlite3.connect(DB_NAME, timeout=15)
            conn.execute('PRAGMA journal_mode=WAL;')
            cursor = conn.cursor()

            updated_symbols = 0
            for signal in signals_data:
                try:
                    # Parse the new JSON format
                    signal_id = signal['signal_id']
                    symbol = signal['symbol']
                    side = signal['signal_side'].upper() # 'BUY' or 'SELL'
                    price = float(signal['entry_price'])
                    date_string = signal['creation_time_utc']
                    dt_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    timestamp = int(dt_object.timestamp())

                    # 1. Check if this signal_id has been processed before.
                    cursor.execute('SELECT signal_id FROM processed_signal_ids WHERE signal_id = ?', (signal_id,))
                    if cursor.fetchone() is not None:
                        log(f"  -> Ignoring duplicate signal_id: {signal_id} for {symbol}")
                        continue

                    # 2. If signal_id is new, INSERT OR REPLACE the signal for the symbol.
                    # This is the core of the new logic.
                    cursor.execute('''
                        INSERT OR REPLACE INTO signals (symbol, side, price, timestamp, status, signal_id, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (symbol, side, price, timestamp, 'new', signal_id))

                    # 3. Add the new signal_id to the processed list.
                    cursor.execute('INSERT INTO processed_signal_ids (signal_id) VALUES (?)', (signal_id,))
                    
                    log(f"  -> 🎉 Stored/Updated signal for {symbol} with new signal_id: {signal_id}")
                    updated_symbols += 1

                except (KeyError, ValueError) as e:
                    log(f"  -> ⚠️  Warning: Signal with unexpected format. Skipping. Data: {signal}, Error: {e}")

            conn.commit()
            if updated_symbols > 0:
                log(f"✅ Finished processing. {updated_symbols} symbol(s) were updated.")
        else:
            error_msg = f"<b>⚠️ API Request Failed (Listener)</b>\n\nStatus Code: {response.status_code}"
            log(error_msg)
            send_message(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"<b>❌ Network Error (Listener)</b>\n\nCould not connect to API.\n\n<b>Error:</b>\n<code>{e}</code>"
        log(error_msg)
        send_message(error_msg)
    except sqlite3.Error as e:
        error_msg = f"<b>❌ Database Error (Listener)</b>\n\n<b>Error:</b>\n<code>{e}</code>"
        log(error_msg)
        send_message(error_msg)
    except Exception as e:
        error_msg = f"<b>❌ Unexpected Error (Listener)</b>\n\n<b>Error:</b>\n<code>{e}</code>"
        log(error_msg)
        send_message(error_msg)
    finally:
        if conn:
            conn.close()

# --- Main part of the script ---
if __name__ == "__main__":
    setup_database()
    log("\n🚀 Starting the signal listener with new architecture... Press Ctrl+C to stop.")
    send_message("<b>🚀 Listener Bot Started (New Architecture)</b>")

    while True:
        try:
            log(f"\n--- Checking for signals ---")
            fetch_and_store_signals()
            log(f"Waiting for {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log("🛑 User interrupted the listener. Shutting down.")
            send_message("<b>🛑 Listener Bot Stopped Manually</b>")
            break
        except Exception as e:
            error_msg = f"<b>❌ CRITICAL ERROR (Listener Loop)</b>\n\n<b>Error:</b>\n<code>{e}</code>"
            log(error_msg)
            send_message(error_msg)
            time.sleep(POLL_INTERVAL)