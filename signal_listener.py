# signal_listener.py (نسخه جدید با MySQL)

import requests
import mysql.connector
import time
from datetime import datetime, timezone
from telegram_logger import send_message
import config

# --- Helper Function for Logging ---
def log(message):
    """Prints a message with a standard UTC timestamp."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

def create_db_connection():
    """Creates a new connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME,
            port=config.DB_PORT
        )
        return conn
    except mysql.connector.Error as e:
        error_msg = f"<b>❌ CRITICAL ERROR (Listener)</b>\n\nFailed to connect to MySQL DB.\n\n<b>Error:</b>\n<code>{e}</code>"
        log(f"CRITICAL ERROR during DB connection: {e}")
        send_message(error_msg)
        return None

def setup_database(conn):
    """Creates the necessary tables in the database if they don't exist."""
    try:
        cursor = conn.cursor()
        # جدول اصلی سیگنال‌ها
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                symbol VARCHAR(20) PRIMARY KEY,
                side VARCHAR(10) NOT NULL,
                price DOUBLE NOT NULL,
                timestamp BIGINT NOT NULL,
                status VARCHAR(20) NOT NULL,
                signal_id VARCHAR(50) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        ''')
        # جدولی برای نگهداری تمام signal_id های دیده شده
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_signal_ids (
                signal_id VARCHAR(50) PRIMARY KEY
            )
        ''')
        conn.commit()
        log("✅ Database tables are ready.")
    except mysql.connector.Error as e:
        error_msg = f"<b>❌ CRITICAL ERROR (Listener)</b>\n\nFailed during database setup.\n\n<b>Error:</b>\n<code>{e}</code>"
        log(f"CRITICAL ERROR during database setup: {e}")
        send_message(error_msg)

def fetch_and_store_signals():
    """Fetches signals and stores them in the MySQL database."""
    conn = None
    try:
        response = requests.get(config.API_URL, timeout=10)
        if response.status_code == 200:
            signals_data = response.json()
            if not signals_data:
                log("No new signals received from API.")
                return

            if isinstance(signals_data, dict):
                signals_data = [signals_data]

            log(f"Received {len(signals_data)} signal(s) from API. Processing...")

            conn = create_db_connection()
            if not conn: return
            
            cursor = conn.cursor()
            updated_symbols = 0

            for signal in signals_data:
                try:
                    signal_id = signal['signal_id']
                    symbol = signal['symbol']
                    side = signal['signal_side'].upper()
                    price = float(signal['entry_price'])
                    dt_object = datetime.strptime(signal['creation_time_utc'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    timestamp = int(dt_object.timestamp())

                    # 1. Check if signal_id has been processed
                    cursor.execute('SELECT signal_id FROM processed_signal_ids WHERE signal_id = %s', (signal_id,))
                    if cursor.fetchone():
                        log(f"  -> Ignoring duplicate signal_id: {signal_id} for {symbol}")
                        continue

                    # 2. INSERT OR UPDATE the signal for the symbol (MySQL syntax)
                    # This is known as "INSERT ... ON DUPLICATE KEY UPDATE"
                    insert_query = '''
                        INSERT INTO signals (symbol, side, price, timestamp, status, signal_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        side = VALUES(side), price = VALUES(price), timestamp = VALUES(timestamp),
                        status = VALUES(status), signal_id = VALUES(signal_id)
                    '''
                    cursor.execute(insert_query, (symbol, side, price, timestamp, 'new', signal_id))

                    # 3. Add the new signal_id to the processed list
                    cursor.execute('INSERT INTO processed_signal_ids (signal_id) VALUES (%s)', (signal_id,))
                    
                    log(f"  -> 🎉 Stored/Updated signal for {symbol} with new signal_id: {signal_id}")
                    updated_symbols += 1

                except (KeyError, ValueError) as e:
                    log(f"  -> ⚠️  Warning: Signal with unexpected format. Skipping. Data: {signal}, Error: {e}")

            conn.commit()
            if updated_symbols > 0:
                log(f"✅ Finished processing. {updated_symbols} symbol(s) were updated.")
        else:
            log(f"API Request Failed. Status Code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        log(f"Network Error: Could not connect to API. {e}")
    except mysql.connector.Error as e:
        log(f"Database Error: {e}")
    except Exception as e:
        log(f"Unexpected Error: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

# --- Main part of the script ---
if __name__ == "__main__":
    # Run setup once at the start
    db_conn = create_db_connection()
    if db_conn:
        setup_database(db_conn)
        db_conn.close()
        log("\n🚀 Starting the signal listener (MySQL)... Press Ctrl+C to stop.")
        send_message("<b>🚀 Listener Bot Started (MySQL Version)</b>")
    else:
        log("Could not start listener due to DB connection failure.")
        exit() # Exit if DB connection fails at start

    while True:
        try:
            log(f"\n--- Checking for signals ---")
            fetch_and_store_signals()
            log(f"Waiting for {config.POLL_INTERVAL if hasattr(config, 'POLL_INTERVAL') else 10} seconds...")
            time.sleep(config.POLL_INTERVAL if hasattr(config, 'POLL_INTERVAL') else 10)
        except KeyboardInterrupt:
            log("🛑 User interrupted the listener. Shutting down.")
            send_message("<b>🛑 Listener Bot Stopped Manually</b>")
            break
        except Exception as e:
            error_msg = f"<b>❌ CRITICAL ERROR (Listener Loop)</b>\n\n<b>Error:</b>\n<code>{e}</code>"
            log(error_msg)
            send_message(error_msg)
            time.sleep(config.POLL_INTERVAL if hasattr(config, 'POLL_INTERVAL') else 10)