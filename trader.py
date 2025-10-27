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
                            params_close = {'reduceOnly': True, 'market_
