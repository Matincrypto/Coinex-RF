# trader.py (نسخه نهایی و اصلاح شده)

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

# --- Helper Function for Console Logging ---
def log(message):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

def get_new_signal(conn):
    """
    Fetches one new signal from the DB.
    The query is updated to work with the new schema (no 'id' column).
    """
    cursor = conn.cursor()
    # ستون signal_id برای لاگ و پیگیری انتخاب می‌شود
    cursor.execute("SELECT symbol, side, price, timestamp, signal_id FROM signals WHERE status = 'new' ORDER BY updated_at ASC LIMIT 1")
    return cursor.fetchone()

def update_signal_status(conn, symbol, new_status):
    """
    Updates signal status using 'symbol' as the key, instead of the old 'id'.
    """
    cursor = conn.cursor()
    cursor.execute("UPDATE signals SET status = ? WHERE symbol = ?", (new_status, symbol))
    conn.commit()

def get_active_positions(exchange):
    """
    Fetches currently open positions from the SWAP market on the exchange.
    """
    try:
        log("-> Fetching open positions from CoinEx (Swap)...")
        # FIX: Explicitly specify 'type': 'swap' to fetch from the correct market
        params = {'type': 'swap'}
        all_positions = exchange.fetch_positions(None, params)
        
        open_positions = [p for p in all_positions if float(p.get('contracts', p.get('size', 0))) > 0]
        
        active_positions = {
            pos['symbol']: {
                'side': pos['side'],
                'contracts': float(pos.get('contracts', pos.get('size', 0))),
                'entryPrice': float(pos['entryPrice']),
                'info': pos['info']
            } for pos in open_positions
        }
        
        if active_positions:
            log(f"-> Found {len(active_positions)} open position(s): {list(active_positions.keys())}")
        else:
            log("-> No open positions found.")
            
        return active_positions
    except Exception as e:
        log(f"❌ Could not fetch positions from exchange: {e}")
        send_message(f"<b>⚠️ Warning: Could not fetch positions</b>\n\n<b>Error:</b>\n<code>{e}</code>")
        return None

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
        f"<b>✅ Trader Bot Started (Stateless - Fixed)</b>\n\n"
        f"<b>Margin:</b> ${config.USDT_AMOUNT}\n"
        f"<b>Leverage:</b> {config.LEVERAGE}x"
    )
    send_message(start_message)

    while True:
        conn = None
        try:
            active_positions = get_active_positions(exchange)
            if active_positions is None:
                time.sleep(POLL_INTERVAL)
                continue

            conn = sqlite3.connect(DB_NAME, timeout=15)
            conn.execute('PRAGMA journal_mode=WAL;')
            
            signal = get_new_signal(conn)

            if not signal:
                log("No new signals to process.")
            else:
                # Unpacking the new tuple structure from get_new_signal
                symbol, side, price, signal_timestamp, signal_id = signal
                order_side = side.lower()
                
                log(f"🔥 Processing signal ID: {signal_id} for Symbol: {symbol}, Side: {order_side}, Price: {price}")

                current_utc_time = datetime.now(timezone.utc)
                signal_utc_time = datetime.fromtimestamp(float(signal_timestamp), tz=timezone.utc)
                time_difference = current_utc_time - signal_utc_time
                
                if time_difference.total_seconds() > (MAX_SIGNAL_AGE_MINUTES * 60):
                    log(f"🟡 SKIPPING (Burnt Signal): Signal is older than {MAX_SIGNAL_AGE_MINUTES} minutes.")
                    update_signal_status(conn, symbol, 'processed_burnt') # Using symbol
                    continue
                
                if symbol in active_positions:
                    existing_position = active_positions[symbol]
                    if existing_position['side'] != order_side:
                        log(f"-> Reverse signal detected! Closing existing {existing_position['side'].upper()} position.")
                        try:
                            close_side = 'sell' if existing_position['side'] == 'buy' else 'buy'
                            params = {'reduceOnly': True}
                            closing_order = exchange.create_order(
                                symbol, 'limit', close_side, existing_position['contracts'], price, params
                            )
                            log(f"   ✅ Closing order placed. ID: {closing_order['id']}")
                            
                            send_message(f"<b>⏳ Position Closing (Reversing)</b>\n\n"
                                         f"<b>Symbol:</b> {symbol}\n<b>Side:</b> {existing_position['side'].upper()}\n"
                                         f"<b>Amount:</b> {existing_position['contracts']}\n<b>Close Price:</b> {price}")
                            
                            time.sleep(5)
                        except Exception as e:
                            log(f"   ❌ CRITICAL: Failed to close position. Error: {e}")
                            update_signal_status(conn, symbol, 'processed_error') # Using symbol
                            send_message(f"<b>❌ CRITICAL: Failed to Close Position</b>\n\n"
                                         f"<b>Symbol:</b> {symbol}\n<b>Error:</b>\n<code>{e}</code>")
                            continue
                    else:
                        log(f"-> Signal has same side as active position. Skipping.")
                        update_signal_status(conn, symbol, 'processed_duplicate') # Using symbol
                        continue

                log("-> Proceeding to open new position.")
                total_position_value = config.USDT_AMOUNT * config.LEVERAGE
                amount_to_trade = total_position_value / price
                
                new_order = exchange.create_order(symbol, 'limit', order_side, amount_to_trade, price)
                log(f"✅ New position opened successfully! ID: {new_order['id']}")

                send_message(f"<b>{'📈' if order_side == 'buy' else '📉'} New Position Opened ({order_side.upper()})</b>\n\n"
                             f"<b>Symbol:</b> {symbol}\n<b>Price:</b> {price}\n"
                             f"<b>Amount:</b> {amount_to_trade:.6f}\n<b>Value:</b> ${total_position_value:.2f}")
                
                update_signal_status(conn, symbol, 'processed') # Using symbol

        except sqlite3.Error as e:
            log(f"❌ Database Error in trader: {e}")
            send_message(f"<b>❌ Database Error (Trader)</b>\n\n<b>Error:</b>\n<code>{e}</code>")
        except ccxt.BaseError as e:
            log(f"❌ Exchange Error in trader: {e}")
            send_message(f"<b>⚠️ Exchange Warning</b>\n\nAn error occurred with CoinEx.\n\n<b>Error:</b>\n<code>{e}</code>")
        except KeyboardInterrupt:
            log("🛑 User interrupted the process. Shutting down.")
            send_message("<b>🛑 Trader Bot Stopped Manually</b>")
            break
        except Exception as e:
            log(f"❌ An unexpected error occurred in the main loop: {e}")
            send_message(f"<b>❌ CRITICAL ERROR (Trader Loop)</b>\n\nBot stopped.\n\n<b>Error:</b>\n<code>{e}</code>")
            break
        finally:
            if conn:
                conn.close()
            
            log(f"Waiting for {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()