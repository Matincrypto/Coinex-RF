# trader.py - Final Version with Correct DB Connection Handling
import ccxt
import sqlite3
import time
from datetime import datetime, timezone
import config

# --- Global Variables ---
DB_NAME = "signals.db"
POLL_INTERVAL = 10  # Synced with the listener's interval
MAX_SIGNAL_AGE_MINUTES = 5
active_positions = {}

# --- Helper Function for Logging ---
def log(message):
    """Prints a message with a standard UTC timestamp."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

def get_new_signals(conn):
    """Fetches new signals from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol, side, price, timestamp FROM signals WHERE status = 'new'")
    return cursor.fetchall()

def update_signal_status(conn, signal_id, new_status='processed'):
    """Updates a signal's status."""
    cursor = conn.cursor()
    cursor.execute("UPDATE signals SET status = ? WHERE id = ?", (new_status, signal_id))
    conn.commit()

def main():
    """The main function containing the bot's core logic."""
    log("🚀 Initializing CoinEx connection...")
    exchange = ccxt.coinex({
        'apiKey': config.COINEX_ACCESS_ID,
        'secret': config.COINEX_SECRET_KEY,
        'options': {'defaultType': 'swap'},
    })
    log("✅ CoinEx connection successful.")
    if config.LEVERAGE <= 0:
        log("❌ ERROR: Leverage is set to 0. Please set a valid leverage.")
        return
        
    log(f"🤖 Trader bot started with ${config.USDT_AMOUNT} margin and {config.LEVERAGE}x leverage...")
    
    while True:
        conn = None  # Reset connection variable at the start of the loop
        try:
            # FIX: Connect to the database INSIDE the loop to get a fresh view of the data
            conn = sqlite3.connect(DB_NAME)
            new_signals = get_new_signals(conn)

            if not new_signals:
                log("No new signals to process.")
            else:
                log(f"Found {len(new_signals)} new signals.")

            for signal in new_signals:
                signal_id, symbol, side, price, signal_timestamp = signal
                order_side = side.lower()
                
                log(f"🔥 Processing task! Signal ID: {signal_id}, Symbol: {symbol}, Side: {order_side}, Price: {price}")

                # --- Stale Signal Validation ---
                current_utc_time = datetime.now(timezone.utc)
                signal_utc_time = datetime.fromtimestamp(float(signal_timestamp), tz=timezone.utc)
                time_difference = current_utc_time - signal_utc_time
                
                if time_difference.total_seconds() > (MAX_SIGNAL_AGE_MINUTES * 60):
                    log(f"🟡 SKIPPING (Burnt Signal): Signal is older than {MAX_SIGNAL_AGE_MINUTES} minutes.")
                    update_signal_status(conn, signal_id, 'processed_burnt')
                    continue
                
                # --- Reversing Logic ---
                if symbol in active_positions:
                    existing_position = active_positions[symbol]
                    if existing_position['side'] != order_side:
                        log(f"-> Reverse signal detected! Closing existing {existing_position['side'].upper()} position.")
                        try:
                            close_side = 'sell' if existing_position['side'] == 'buy' else 'buy'
                            closing_order = exchange.create_order(
                                symbol, 'limit', close_side, existing_position['amount'], price, {'reduceOnly': True}
                            )
                            log(f"   ✅ Closing order placed. ID: {closing_order['id']}")
                            time.sleep(5)
                        except Exception as e:
                            log(f"   ❌ CRITICAL: Failed to close position. Error: {e}")
                            update_signal_status(conn, signal_id, 'processed_error')
                            continue
                        del active_positions[symbol]
                    else:
                        log(f"-> Signal side is the same. Skipping.")
                        update_signal_status(conn, signal_id)
                        continue

                # --- Open New Position ---
                log("-> Proceeding to open new position.")
                try:
                    exchange.set_margin_mode('isolated', symbol)
                    exchange.set_leverage(config.LEVERAGE, symbol)
                except Exception as e:
                    log(f"⚠️ Warning: Could not set margin/leverage. Error: {e}")

                total_position_value = config.USDT_AMOUNT * config.LEVERAGE
                amount_to_trade = total_position_value / price
                
                log(f"   -> Placing new {order_side.upper()} order for {amount_to_trade:.8f}...")
                new_order = exchange.create_order(symbol, 'limit', order_side, amount_to_trade, price)
                log(f"✅ New position opened successfully! ID: {new_order['id']}")

                active_positions[symbol] = new_order
                log(f"   -> New position state saved to memory.")
                
                update_signal_status(conn, signal_id)

        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully
            log("🛑 User interrupted the process. Shutting down.")
            break
        except Exception as e:
            log(f"❌ An unexpected error occurred in the main loop: {e}")
        finally:
            # Ensure the connection is always closed after each cycle
            if conn:
                conn.close()
            
            # This check prevents sleeping after a KeyboardInterrupt
            if 'e' in locals() and isinstance(e, KeyboardInterrupt):
                pass
            else:
                log(f"Waiting for {POLL_INTERVAL} seconds...")
                time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
