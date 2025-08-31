# trader.py - Final Version
import ccxt
import sqlite3
import time
import config

# --- Global Variables ---
DB_NAME = "signals.db"
POLL_INTERVAL = 5
active_positions = {}

def get_new_signals(conn):
    """Fetches signals with 'new' status from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol, side, price FROM signals WHERE status = 'new'")
    return cursor.fetchall()

def update_signal_status(conn, signal_id):
    """Updates a signal's status to 'processed'."""
    cursor = conn.cursor()
    cursor.execute("UPDATE signals SET status = 'processed' WHERE id = ?", (signal_id,))
    conn.commit()

def main():
    """The main function containing the bot's core logic."""
    print("🚀 Initializing CoinEx connection...")
    exchange = ccxt.coinex({
        'apiKey': config.COINEX_ACCESS_ID,
        'secret': config.COINEX_SECRET_KEY,
        'options': {'defaultType': 'swap'},
    })
    print("✅ CoinEx connection successful.")
    print(f"🤖 Trader bot started. Trading with ${config.USDT_AMOUNT} margin and {config.LEVERAGE}x leverage...")
    
    conn = sqlite3.connect(DB_NAME)

    while True:
        try:
            new_signals = get_new_signals(conn)

            for signal in new_signals:
                signal_id, symbol, side, price = signal
                
                print(f"\n🔥 New task received! Signal ID: {signal_id}, Symbol: {symbol}, Side: {side}, Price: {price}")

                # --- Leverage and Amount Calculation ---
                order_side = side.lower()
                leverage = config.LEVERAGE
                
                # The total position value will be (USDT_AMOUNT * LEVERAGE)
                # The amount to trade is calculated based on this total value.
                base_amount = (config.USDT_AMOUNT * leverage) / price
                
                amount_to_trade = base_amount
                
                # --- Reversing Logic ---
                if symbol in active_positions:
                    if active_positions[symbol] != order_side:
                        print(f"-> Active position found for {symbol} ({active_positions[symbol]}). Reversing.")
                        amount_to_trade = base_amount * 2
                    else:
                        print(f"-> Signal side is the same as the active position. Skipping.")
                        update_signal_status(conn, signal_id)
                        continue
                
                # --- Set Leverage on the Exchange ---
                print(f"-> Setting leverage for {symbol} to {leverage}x...")
                params = {'market_type': 'FUTURES'}
                exchange.set_leverage(leverage, symbol, params)
                print(f"✅ Leverage set to {leverage}x.")
                
                # --- Place The Order ---
                print(f"-> Placing {order_side.upper()} order for {amount_to_trade:.8f} {symbol}...")
                order = exchange.create_order(symbol, 'limit', order_side, amount_to_trade, price, params)
                
                print("✅ Order placed successfully!")
                print(f"   -> Order ID: {order['id']}")

                # Update the bot's memory with the new position's side
                active_positions[symbol] = order_side
                update_signal_status(conn, signal_id)
                print(f"-> Marked Signal ID {signal_id} as 'processed'.")

            time.sleep(POLL_INTERVAL)

        except ccxt.ExchangeError as e:
            print(f"❌ Exchange Error: {e}")
            if 'signal_id' in locals():
                update_signal_status(conn, signal_id) # Mark as processed to avoid repeated errors
            time.sleep(POLL_INTERVAL) # Wait before retrying
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")
            break # Stop on other critical errors
    
    conn.close()
    print("🤖 Trader bot has stopped.")

if __name__ == "__main__":
    main()