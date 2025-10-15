# trader.py (نسخه جدید با گزارش وضعیت دوره‌ای و تکرار هوشمند)

import ccxt
import time
from datetime import datetime, timezone, timedelta # ماژول timedelta اضافه شد
import config
from telegram_logger import send_message
from shared_utils import log, create_db_connection, setup_database

# --- تنظیمات عمومی ---
MAX_SIGNAL_AGE_MINUTES = 5
RETRY_INTERVAL_SECONDS = 30 # فاصله زمانی برای تلاش مجدد هنگام تسویه فاندینگ

# --- تنظیمات جدید برای گزارش وضعیت دوره‌ای ---
HEALTH_CHECK_INTERVAL_MINUTES = 10 # هر چند دقیقه گزارش ارسال شود
last_health_check_time = datetime.now(timezone.utc) # زمان آخرین گزارش

def get_active_positions(exchange):
    """
    پوزیشن‌های باز را از صرافی دریافت می‌کند.
    اگر صرافی در حال تسویه فاندینگ باشد، تا زمان در دسترس شدن، منتظر می‌ماند و دوباره تلاش می‌کند.
    """
    while True:
        try:
            log("۱. در حال دریافت پوزیشن‌های باز از صرافی...")
            params = {'type': 'swap'}
            all_positions = exchange.fetch_positions(None, params)
            open_positions = {p['symbol']: p for p in all_positions if float(p.get('contracts', 0)) > 0}
            if open_positions:
                log(f"   -> {len(open_positions)} پوزیشن باز پیدا شد: {list(open_positions.keys())}")
            else:
                log("   -> هیچ پوزیشن بازی در صرافی وجود ندارد.")
            return open_positions
        except ccxt.BaseError as e:
            if 'funding fee settlement' in str(e):
                log(f"🟡 هشدار صرافی: سرویس در حین تسویه کارمزد فاندینگ در دسترس نیست.")
                log(f"   -> ربات متوقف نمی‌شود. {RETRY_INTERVAL_SECONDS} ثانیه دیگر دوباره تلاش خواهد شد...")
                time.sleep(RETRY_INTERVAL_SECONDS)
            else:
                log(f"❌ خطای جدی در ارتباط با صرافی: {e}")
                send_message(f"<b>❌ خطای جدی در دریافت پوزیشن</b>\n\n<b>خطا:</b>\n<code>{e}</code>")
                return None
        except Exception as e:
            log(f"❌ یک خطای پیش‌بینی نشده در دریافت پوزیشن رخ داد: {e}")
            return None

def get_new_signal(conn):
    """یک سیگنال جدید از دیتابیس MySQL دریافت می‌کند."""
    cursor = conn.cursor(dictionary=True)
    query = "SELECT symbol, side, price, timestamp, signal_id FROM signals WHERE status = 'new' ORDER BY updated_at ASC LIMIT 1"
    cursor.execute(query)
    signal = cursor.fetchone()
    if signal:
        log(f"🔥 سیگنال جدید پیدا شد: {signal['symbol']} | {signal['side']} | شناسه: {signal['signal_id']}")
    return signal

def update_signal_status(conn, symbol, new_status):
    """وضعیت یک سیگنال را در دیتابیس MySQL به‌روزرسانی می‌کند."""
    try:
        cursor = conn.cursor()
        query = "UPDATE signals SET status = %s WHERE symbol = %s"
        cursor.execute(query, (new_status, symbol))
        conn.commit()
        log(f"   -> وضعیت سیگنال {symbol} در دیتابیس به '{new_status}' تغییر کرد.")
    except Exception as e:
        log(f"   -> ❌ خطا در آپدیت وضعیت سیگنال {symbol}. خطا: {e}")
        conn.rollback()

def process_trades(exchange):
    """منطق اصلی پردازش معاملات را اجرا می‌کند."""
    conn = None
    try:
        log("--- شروع چرخه جدید تريد ---")
        open_positions = get_active_positions(exchange)
        if open_positions is None:
            log("   -> پردازش این چرخه به دلیل خطای جدی در دریافت پوزیشن متوقف شد.")
            return

        conn = create_db_connection()
        if not conn:
            log("پردازش به دلیل عدم اتصال به دیتابیس متوقف شد.")
            return

        signal = get_new_signal(conn)
        if not signal:
            log("۲. سیگنال جدیدی برای پردازش در دیتابیس وجود ندارد.")
            return
        
        log(f"۲. در حال پردازش سیگنال برای نماد {signal['symbol']}...")

        signal_age = datetime.now(timezone.utc) - datetime.fromtimestamp(signal['timestamp'], tz=timezone.utc)
        if signal_age.total_seconds() > (MAX_SIGNAL_AGE_MINUTES * 60):
            log(f"🟡 سیگنال نادیده گرفته شد (سوخته). عمر سیگنال: {signal_age}")
            update_signal_status(conn, signal['symbol'], 'processed_burnt')
            return

        symbol, order_side, price = signal['symbol'], signal['side'].lower(), signal['price']
        
        if symbol in open_positions:
            existing_pos = open_positions[symbol]
            log(f"   -> یک پوزیشن '{existing_pos['side'].upper()}' برای {symbol} از قبل باز است.")
            if existing_pos['side'] != order_side:
                log(f"   -> سیگنال معکوس شناسایی شد! در حال بستن پوزیشن فعلی...")
                # ... (بقیه کد بدون تغییر)
            else:
                log("   -> سیگنال هم‌جهت با پوزیشن فعلی است. نادیده گرفته شد.")
                update_signal_status(conn, signal, 'processed_duplicate')
        else:
            log("   -> پوزیشن بازی برای این نماد وجود ندارد. در حال باز کردن پوزیشن جدید...")
            # ... (بقیه کد بدون تغییر)

    except Exception as e:
        log(f"❌ یک خطای پیش‌بینی نشده در چرخه اصلی Trader رخ داد. خطا: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            log("اتصال به دیتابیس بسته شد.")

def send_health_check():
    """یک پیام وضعیت برای اطمینان از فعال بودن ربات به تلگرام ارسال می‌کند."""
    global last_health_check_time
    now = datetime.now(timezone.utc)
    if now - last_health_check_time > timedelta(minutes=HEALTH_CHECK_INTERVAL_MINUTES):
        log("✅ ارسال گزارش وضعیت دوره‌ای به تلگرام...")
        time_str = now.strftime('%Y-%m-%d %H:%M:%S')
        message = (f"<b>- گزارش وضعیت ربات معامله‌گر -</b>\n\n"
                   f"✅ ربات فعال و در حال کار است.\n"
                   f"<b>آخرین بررسی:</b>\n<code>{time_str} UTC</code>")
        send_message(message)
        last_health_check_time = now # به‌روزرسانی زمان آخرین گزارش

if __name__ == "__main__":
    log("🚀 ربات معامله‌گر (Trader) در حال شروع به کار...")
    try:
        exchange = ccxt.coinex({
            'apiKey': config.COINEX_ACCESS_ID,
            'secret': config.COINEX_SECRET_KEY,
            'options': {'defaultType': 'swap'},
        })
        log("✅ با موفقیت به صرافی CoinEx متصل شد.")
        send_message(f"<b>✅ Trader Bot Started (Robust Version)</b>\n\n"
                     f"<b>Margin:</b> ${config.USDT_AMOUNT}\n<b>Leverage:</b> {config.LEVERAGE}x")
        
        db_conn = create_db_connection()
        if db_conn:
            if setup_database(db_conn):
                db_conn.close()
                while True:
                    process_trades(exchange)
                    
                    # --- بخش جدید: ارسال گزارش وضعیت ---
                    send_health_check()
                    # -----------------------------------
                    
                    wait_interval = config.POLL_INTERVAL if hasattr(config, 'POLL_INTERVAL') else 10
                    log(f"--- چرخه تمام شد. انتظار برای {wait_interval} ثانیه... ---")
                    time.sleep(wait_interval)
            else:
                db_conn.close()
                log("❌ ربات به دلیل عدم موفقیت در راه‌اندازی دیتابیس، متوقف شد.")
        else:
            log("❌ ربات به دلیل عدم اتصال به دیتابیس، متوقف شد.")

    except KeyboardInterrupt:
        log("🛑 ربات معامله‌گر با دستور کاربر متوقف شد.")
        send_message("<b>🛑 Trader Bot Stopped Manually</b>")
    except Exception as e:
        log(f"❌ یک خطای حیاتی در هنگام شروع به کار Trader رخ داد. خطا: {e}")
        send_message(f"<b>❌ CRITICAL ERROR (Trader Start)</b>\n\n<b>Error:</b>\n<code>{e}</code>")
