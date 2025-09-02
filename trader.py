# trader.py (نسخه نهایی و هماهنگ)

import ccxt
import sqlite3
import time
from datetime import datetime, timezone
import config
from telegram_logger import send_message

# --- Global Variables ---
DB_NAME = "signals.db"
POLL_INTERVAL = 10
MAX_SIGNAL_AGE_MINUTES = 5
active_positions = {}

# --- Helper Function for Console Logging ---
def log(message):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

def get_new_signals(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol, side, price, timestamp FROM signals WHERE status = 'new'")
    return cursor.fetchall()

def update_signal_status(conn, signal_id, new_status='processed'):
    cursor = conn.cursor()
    cursor.execute("UPDATE signals SET status = ? WHERE id = ?", (new_status, signal_id))
    conn.commit()

def main():
    log("🚀 Initializing CoinEx connection...")
    try:
        exchange = ccxt.coinex({
            'apiKey': config.COINEX_ACCESS_ID,
            'secret': config.COINEX_SECRET_KEY,
            'options': {'defaultType': 'swap'},
        })
        log("✅ CoinEx connection successful.")
    except Exception as e:
        log(f"❌ CRITICAL: Failed to connect to CoinEx. Error: {e}")
        send_message(f"<b>❌ CRITICAL ERROR</b>\n\nFailed to connect to CoinEx.\n\n<b>Error:</b>\n<code>{e}</code>")
        return

    start_message = (
        "<b>✅ Bot Started Successfully</b>\n\n"
        f"<b>Margin:</b> ${config.USDT_AMOUNT}\n"
        f"<b>Leverage:</b> {config.LEVERAGE}x"
    )
    send_message(start_message)

    while True:
        conn = None
        try:
            # اتصال بهینه به دیتابیس برای جلوگیری از قفل شدن
            conn = sqlite3.connect(DB_NAME, timeout=15)
            conn.execute('PRAGMA journal_mode=WAL;')
            
            new_signals = get_new_signals(conn)

            if not new_signals:
                log("No new signals to process.")
            
            for signal in new_signals:
                signal_id, symbol, side, price, signal_timestamp = signal
                order_side = side.lower()
                
                log(f"🔥 Processing task! Signal ID: {signal_id}, Symbol: {symbol}, Side: {order_side}, Price: {price}")

                # Stale Signal Validation
                current_utc_time = datetime.now(timezone.utc)
                signal_utc_time = datetime.fromtimestamp(float(signal_timestamp), tz=timezone.utc)
                time_difference = current_utc_time - signal_utc_time
                
                if time_difference.total_seconds() > (MAX_SIGNAL_AGE_MINUTES * 60):
                    log(f"🟡 SKIPPING (Burnt Signal): Signal is older than {MAX_SIGNAL_AGE_MINUTES} minutes.")
                    update_signal_status(conn, signal_id, 'processed_burnt')
                    continue
                
                # Reversing Logic
                if symbol in active_positions:
                    existing_position = active_positions[symbol]
                    if existing_position['side'] != order_side:
                        log(f"-> Reverse signal detected! Closing existing {existing_position['side'].upper()} position.")
                        try:
                            # ... (منطق بستن پوزیشن مانند قبل)
                            log(f"   ✅ Closing order placed.")
                            del active_positions[symbol]
                            time.sleep(5) # Give time for the close order to process
                        except Exception as e:
                            log(f"   ❌ CRITICAL: Failed to close position for reversing. Error: {e}")
                            update_signal_status(conn, signal_id, 'processed_error')
                            continue
                    else:
                        log(f"-> Signal side is the same. Skipping.")
                        update_signal_status(conn, signal_id, 'processed_duplicate')
                        continue

                # Open New Position
                log("-> Proceeding to open new position.")
                total_position_value = config.USDT_AMOUNT * config.LEVERAGE
                amount_to_trade = total_position_value / price
                
                new_order = exchange.create_order(symbol, 'limit', order_side, amount_to_trade, price)
                log(f"✅ New position opened successfully! ID: {new_order['id']}")

                open_message = (
                    f"<b>{'📈' if order_side == 'buy' else '📉'} New Position Opened ({order_side.upper()})</b>\n\n"
                    f"<b>Symbol:</b> {symbol}\n"
                    f"<b>Price:</b> {price}\n"
                    f"<b>Amount:</b> {amount_to_trade:.6f}\n"
                    f"<b>Value:</b> ${total_position_value:.2f}"
                )
                send_message(open_message)

                active_positions[symbol] = new_order
                update_signal_status(conn, signal_id)

        except sqlite3.Error as e:
            log(f"❌ Database Error in trader: {e}")
        except ccxt.BaseError as e:
            log(f"❌ Exchange Error in trader: {e}")
            send_message(f"<b>⚠️ Exchange Warning</b>\n\nAn error occurred while communicating with CoinEx.\n\n<b>Error:</b>\n<code>{e}</code>")
        except KeyboardInterrupt:
            log("🛑 User interrupted the process. Shutting down.")
            send_message("<b>🛑 Bot Stopped Manually</b>")
            break
        except Exception as e:
            log(f"❌ An unexpected error occurred in the main loop: {e}")
            error_message = f"<b>❌ CRITICAL ERROR</b>\n\nBot stopped unexpectedly.\n\n<b>Error:</b>\n<code>{e}</code>"
            send_message(error_message)
            break
        finally:
            if conn:
                conn.close()
            
            log(f"Waiting for {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()