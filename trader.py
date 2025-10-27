# trader.py (نسخه نهایی با MySQL، تنظیم اهرم، و محاسبه کارمزد دینامیک)

import ccxt
import mysql.connector
import time
from datetime import datetime, timezone
import config  # ایمپورت کردن فایل تنظیمات
from telegram_logger import send_message

# --- Global Variables ---
# این متغیرها اکنون از config.py خوانده می‌شوند یا مستقیماً استفاده می‌شوند
POLL_INTERVAL = config.POLL_INTERVAL if hasattr(config, 'POLL_INTERVAL') else 10  # فاصله زمانی بین هر بررسی (ثانیه)
MAX_SIGNAL_AGE_MINUTES = 5  # حداکثر عمر سیگنال به دقیقه

# --- Helper Function for Console Logging ---
def log(message):
    """یک پیام را با فرمت زمانی استاندارد UTC چاپ می‌کند."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

# --- Database Functions (MySQL Version) ---

def create_db_connection():
    """یک اتصال جدید به دیتابیس MySQL ایجاد می‌کند و آن را برمی‌گرداند."""
    try:
        conn = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME,
            port=config.DB_PORT
        )
        if conn.is_connected():
            # لاگ اتصال موفق را فقط در تابع main نگه می‌داریم تا کنسول شلوغ نشود
            return conn
    except mysql.connector.Error as e:
        log(f"❌ DATABASE CRITICAL: Could not connect to MySQL. Error: {e}")
        send_message(f"<b>❌ CRITICAL: MySQL Connection Failed</b>\n\n<b>Error:</b>\n<code>{e}</code>")
        return None

def get_new_signal(conn):
    """یک سیگنال جدید از دیتابیس MySQL دریافت می‌کند."""
    cursor = conn.cursor()
    query = "SELECT symbol, side, price, timestamp, signal_id FROM signals WHERE status = 'new' ORDER BY updated_at ASC LIMIT 1"
    cursor.execute(query)
    return cursor.fetchone()

def update_signal_status(conn, symbol, new_status):
    """وضعیت یک سیگنال را در دیتابیس MySQL به‌روزرسانی می‌کند."""
    try:
        cursor = conn.cursor()
        query = "UPDATE signals SET status = %s WHERE symbol = %s"
        cursor.execute(query, (new_status, symbol))
        conn.commit()
        log(f"   -> DB: Updated status for {symbol} to '{new_status}'.")
    except mysql.connector.Error as e:
        log(f"   -> ❌ DB ERROR: Failed to update status for {symbol}. Error: {e}")
        conn.rollback()

# --- Exchange Functions ---

def get_active_positions(exchange):
    """موقعیت‌های باز فعلی را از صرافی دریافت می‌کند."""
    try:
        log("-> Fetching open positions from CoinEx (Swap)...")
        params = {'type': 'swap'}
        all_positions = exchange.fetch_positions(None, params)
        
        open_positions = [p for p in all_positions if float(p.get('contracts', p.get('size', 0))) > 0]
        
        active_positions = {
            pos['symbol']: {
                'side': pos['side'],
                'contracts': float(pos.get('contracts', pos.get('size', 0))),
                'entryPrice': float(pos['entryPrice'])
            } for pos in open_positions
        }
        
        if active_positions:
            log(f"-> Found {len(active_positions)} open position(s): {list(active_positions.keys())}")
        else:
            log("-> No open positions found.")
            
        return active_positions
    except Exception as e:
        log(f"❌ EXCHANGE ERROR: Could not fetch positions: {e}")
        send_message(f"<b>⚠️ Warning: Could not fetch positions</b>\n\n<b>Error:</b>\n<code>{e}</code>")
        return None

# --- Main Bot Logic ---

def main():
    log("🚀 Initializing CoinEx connection...")
    try:
        exchange = ccxt.coinex({
            'apiKey': config.COINEX_ACCESS_ID,
            'secret': config.COINEX_SECRET_KEY,
            'options': {'defaultType': 'swap'},
        })
        
        # --- (اصلاحیه ۱) بارگذاری اطلاعات بازار ---
        log("-> در حال بارگذاری اطلاعات بازار (کارمزد، دقت، ...)")
        exchange.load_markets() 
        log("✅ اتصال به CoinEx موفق و اطلاعات بازار بارگذاری شد.")
        # ----------------------------------------
        
    except Exception as e:
        log(f"❌ CRITICAL: Failed to connect or load markets. Shutting down. Error: {e}")
        send_message(f"<b>❌ CRITICAL ERROR</b>\n\nFailed to connect to CoinEx or load markets.\n\n<b>Error:</b>\n<code>{e}</code>")
        return

    start_message = (
        f"<b>✅ Trader Bot Started (MySQL Version)</b>\n\n"
        f"<b>Margin:</b> ${config.USDT_AMOUNT}\n"
        f"<b>Leverage:</b> {config.LEVERAGE}x"
    )
    send_message(start_message)

    db_conn_main = None
    try:
        db_conn_main = create_db_connection()
        if db_conn_main and db_conn_main.is_connected():
            log("✅ Successfully connected to MySQL database.")
        else:
            log("❌ Database connection failed at start. Exiting.")
            return
    finally:
        if db_conn_main and db_conn_main.is_connected():
            db_conn_main.close()


    while True:
        conn = None
        try:
            # 1. اتصال به دیتابیس
            conn = create_db_connection()
            if not conn:
                log("Database connection failed. Retrying in a moment...")
                time.sleep(POLL_INTERVAL)
                continue

            # 2. دریافت موقعیت‌های فعال از صرافی
            active_positions = get_active_positions(exchange)
            if active_positions is None:
                log("Could not get active positions. Skipping this cycle.")
                time.sleep(POLL_INTERVAL)
                continue

            # 3. دریافت سیگنال جدید از دیتابیس
            signal = get_new_signal(conn)

            if not signal:
                log("No new signals to process.")
            else:
                symbol, side, price, signal_timestamp, signal_id = signal
                order_side = side.lower()
                
                log(f"🔥 Processing Signal ID: {signal_id} | Symbol: {symbol} | Side: {order_side} | Price: {price}")

                # 4. بررسی تاریخ انقضای سیگنال (سیگنال سوخته)
                current_utc_time = datetime.now(timezone.utc)
                signal_utc_time = datetime.fromtimestamp(float(signal_timestamp), tz=timezone.utc)
                time_difference = current_utc_time - signal_utc_time
                
                if time_difference.total_seconds() > (MAX_SIGNAL_AGE_MINUTES * 60):
                    log(f"🟡 SKIPPING (Burnt Signal): Signal is older than {MAX_SIGNAL_AGE_MINUTES} minutes.")
                    update_signal_status(conn, symbol, 'processed_burnt')
                    continue
                
                # 5. بررسی منطق معاملاتی بر اساس موقعیت‌های باز
                if symbol in active_positions:
                    existing_position = active_positions[symbol]
                    if existing_position['side'] != order_side:
                        
                        # --- (اصلاحیه ۲) منطق امن‌تر برای بستن پوزیشن معکوس ---
                        log(f"-> Reverse signal detected! Attempting to close existing {existing_position['side'].upper()} position for {symbol}.")
                        try:
                            close_side = 'sell' if existing_position['side'] == 'buy' else 'buy'
                            # استفاده از Market Order برای بستن فوری و تضمینی پوزیشن
                            # reduceOnly=True تضمین می‌کند که فقط پوزیشن فعلی بسته شود و پوزیشن جدید باز نشود
                            
                            # --- (خطای شما اینجا بود - تصحیح شد) ---
                            params_close = {'reduceOnly': True, 'market_type': 'FUTURES'} 
                            # -------------------------------------

                            log(f"   -> Placing MARKET order to close {symbol}...")
                            
                            closing_order = exchange.create_order(symbol, 'market', close_side, existing_position['contracts'], None, params_close)
                            
                            log(f"   ✅ Closing order placed. ID: {closing_order['id']}")
                            send_message(f"<b>⏳ Position Closing (Reversing)</b>\n\n"
                                         f"<b>Symbol:</b> {symbol}\n<b>Side:</b> {existing_position['side'].upper()}\n"
                                         f"<b>Amount:</b> {existing_position['contracts']}\n<b>Type:</b> MARKET")
                            
                            update_signal_status(conn, symbol, 'processed_closed_old')
                            time.sleep(3) # تاخیر کوتاه برای پردازش سفارش توسط صرافی
                            continue # برو به ابتدای حلقه و پوزیشن جدید باز نکن

                        except Exception as e:
                            log(f"   ❌ CRITICAL: Failed to close reverse position for {symbol}. Error: {e}")
                            update_signal_status(conn, symbol, 'processed_error')
                            send_message(f"<b>❌ CRITICAL: Failed to Close Position</b>\n\n"
                                         f"<b>Symbol:</b> {symbol}\n<b>Error:</b>\n<code>{e}</code>")
                            continue # در صورت خطا، حتما ادامه بده و پوزیشن جدید باز نکن
                        # ----------------------------------------------------
                            
                    else:
                        log(f"-> Signal has same side as active position for {symbol}. Skipping.")
                        update_signal_status(conn, symbol, 'processed_duplicate')
                        continue

                # --- (اصلاحیه ۳) بخش کامل باز کردن موقعیت جدید ---
                log(f"-> Proceeding to open new {order_side.upper()} position for {symbol}.")
                try:
                    # ۱. دریافت اطلاعات بازار که در ابتدا لود کردیم
                    market_info = exchange.markets.get(symbol)
                    if not market_info:
                        log(f"   ❌ ERROR: No market data found for {symbol} (was not loaded). Skipping.")
                        update_signal_status(conn, symbol, 'processed_error')
                        continue
                        
                    # ۲. محاسبه دینامیک بافر کارمزد
                    taker_fee_rate = float(market_info['taker'])
                    FEE_BUFFER = taker_fee_rate + 0.0005  # e.g., 0.002 (0.2%) + 0.0005 (0.05%) = 0.0025 (0.25%)
                    adjusted_usdt_amount = config.USDT_AMOUNT * (1 - FEE_BUFFER)

                    # ۳. تنظیم اهرم (Leverage)
                    log(f"-> Setting leverage for {symbol} to {config.LEVERAGE}x...")
                    params_open = {'market_type': 'FUTURES'} 
                    try:
                        exchange.set_leverage(config.LEVERAGE, symbol, params_open)
                        log(f"   ✅ Leverage set successfully to {config.LEVERAGE}x.")
                    except Exception as e:
                        log(f"   ❌ WARNING: Failed to set leverage for {symbol}. Error: {e}")
                        send_message(f"<b>⚠️ Warning: Failed to Set Leverage</b>\n\n<b>Symbol:</b> {symbol}\n<b>Error:</b>\n<code>{e}</code>")
                        update_signal_status(conn, symbol, 'processed_error')
                        continue # اگر تنظیم اهرم شکست خورد، ادامه نده

                    # ۴. محاسبه مقدار (Amount)
                    total_position_value = adjusted_usdt_amount * config.LEVERAGE
                    raw_amount_to_trade = total_position_value / price # مقدار خام
                    
                    # ۵. اعمال دقت اعشار و قیمت (Formatting)
                    amount_to_trade_str = exchange.amount_to_precision(symbol, raw_amount_to_trade)
                    price_to_trade_str = exchange.price_to_precision(symbol, price)
                    
                    amount_to_trade_float = float(amount_to_trade_str)
                    price_to_trade_float = float(price_to_trade_str)


                    # ۶. بررسی حداقل مقدار سفارش (Min Amount Check)
                    min_amount = float(market_info['limits']['amount']['min'])
                    if amount_to_trade_float < min_amount:
                        log(f"   ❌ ERROR: Calculated amount {amount_to_trade_float} is less than min_amount {min_amount} for {symbol}. Skipping.")
                        send_message(f"<b>❌ Order Error: Amount Too Small</b>\n\n<b>Symbol:</b> {symbol}\n<b>Calculated:</b> {amount_to_trade_float}\n<b>Min:</b> {min_amount}")
                        update_signal_status(conn, symbol, 'processed_error')
                        continue

                    # --- لاگ نهایی قبل از ارسال ---
                    log(f"   -> Adjusted Margin: ${adjusted_usdt_amount:.2f} (Fee Buffer: {FEE_BUFFER*100:.3f}%)")
                    log(f"   -> Target Position Value: ${total_position_value:.2f}")
                    log(f"   -> Raw Amount: {raw_amount_to_trade} -> Formatted Amount: {amount_to_trade_str}")
                    log(f"   -> Raw Price: {price} -> Formatted Price: {price_to_trade_str}")

                    # ۷. ثبت سفارش نهایی
                    new_order = exchange.create_order(symbol, 'limit', order_side, amount_to_trade_float, price_to_trade_float, params_open)
                    
                    log(f"✅ New position opened successfully! ID: {new_order['id']}")

                    send_message(f"<b>{'📈' if order_side == 'buy' else '📉'} New Position Opened ({order_side.upper()})</b>\n\n"
                                 f"<b>Symbol:</b> {symbol}\n<b>Price:</b> {price_to_trade_str}\n"
                                 f"<b>Amount:</b> {amount_to_trade_str}\n<b>Value:</b> ${total_position_value:.2f}")
                    
                    update_signal_status(conn, symbol, 'processed')
                
                except Exception as e:
                    log(f"❌ CRITICAL: Failed to open new position for {symbol}. Error: {e}")
                    update_signal_status(conn, symbol, 'processed_error')
                    send_message(f"<b>❌ CRITICAL: Failed to Open Position</b>\n\n"
                                 f"<b>Symbol:</b> {symbol}\n<b>Error:</b>\n<code>{e}</code>")
                # ----------------------------------------------------

        except mysql.connector.Error as e:
            log(f"❌ DATABASE ERROR in main loop: {e}")
            send_message(f"<b>❌ Database Error (Trader Loop)</b>\n\n<b>Error:</b>\n<code>{e}</code>")
        except ccxt.BaseError as e:
            log(f"❌ EXCHANGE ERROR in main loop: {e}")
            send_message(f"<b>⚠️ Exchange Warning</b>\n\nAn error occurred with CoinEx.\n\n<b>Error:</b>\n<code>{e}</code>")
        except KeyboardInterrupt:
            log("🛑 User interrupted the process. Shutting down.")
            send_message("<b>🛑 Trader Bot Stopped Manually</b>")
            break
        except Exception as e:
            log(f"❌ An unexpected critical error occurred in the main loop: {e}")
            send_message(f"<b>❌ CRITICAL ERROR (Trader Loop)</b>\n\nBot will be stopped.\n\n<b>Error:</b>\n<code>{e}</code>")
            break # در خطاهای ناشناخته بهتر است ربات متوقف شود
        finally:
            if conn and conn.is_connected():
                conn.close()
                # لاگ بسته شدن دیتابیس را حذف می‌کنیم تا کنسول خلوت بماند
            
            log(f"--- Cycle finished. Waiting for {POLL_INTERVAL} seconds... ---")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
