# signal_listener.py (نسخه جدید با لاگ فارسی و دیتابیس هوشمند)

import requests
import time
from datetime import datetime, timezone
from telegram_logger import send_message
import config
from shared_utils import log, create_db_connection, setup_database

def fetch_and_store_signals():
    """سیگنال‌ها را دریافت و در دیتابیس MySQL ذخیره می‌کند."""
    conn = None
    try:
        log("--- شروع چرخه جدید دریافت سیگنال ---")
        log("۱. در حال ارسال درخواست به API سیگنال...")
        response = requests.get(config.API_URL, timeout=10)

        if response.status_code == 200:
            signals_data = response.json()
            if not signals_data:
                log(" API سیگنال جدیدی ارسال نکرده است.")
                return

            if isinstance(signals_data, dict):
                signals_data = [signals_data]

            log(f" API تعداد {len(signals_data)} سیگنال ارسال کرد. در حال پردازش...")
            
            conn = create_db_connection()
            if not conn:
                log("پردازش به دلیل عدم اتصال به دیتابیس متوقف شد.")
                return
            
            cursor = conn.cursor()
            updated_symbols = 0

            for signal in signals_data:
                try:
                    signal_id = signal['signal_id']
                    symbol = signal['symbol']
                    log(f"-> پردازش سیگنال برای {symbol} با شناسه: {signal_id}")

                    # ۱. بررسی تکراری بودن شناسه سیگنال
                    cursor.execute('SELECT signal_id FROM processed_signal_ids WHERE signal_id = %s', (signal_id,))
                    if cursor.fetchone():
                        log(f"  - نادیده گرفته شد: شناسه سیگنال '{signal_id}' تکراری است.")
                        continue

                    # ۲. استخراج و تبدیل اطلاعات سیگنال
                    side = signal['signal_side'].upper()
                    price = float(signal['entry_price'])
                    dt_object = datetime.strptime(signal['creation_time_utc'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    timestamp = int(dt_object.timestamp())
                    log(f"  - اطلاعات سیگنال: جهت={side}, قیمت={price}, زمان={dt_object}")

                    # ۳. درج یا به‌روزرسانی سیگنال در جدول اصلی
                    insert_query = '''
                        INSERT INTO signals (symbol, side, price, timestamp, status, signal_id)
                        VALUES (%s, %s, %s, %s, 'new', %s)
                        ON DUPLICATE KEY UPDATE
                        side = VALUES(side), price = VALUES(price), timestamp = VALUES(timestamp),
                        status = 'new', signal_id = VALUES(signal_id)
                    '''
                    cursor.execute(insert_query, (symbol, side, price, timestamp, signal_id))
                    log(f"  - سیگنال برای نماد {symbol} در جدول 'signals' با موفقیت درج/به‌روزرسانی شد.")

                    # ۴. افزودن شناسه به لیست پردازش‌شده‌ها
                    cursor.execute('INSERT INTO processed_signal_ids (signal_id) VALUES (%s)', (signal_id,))
                    log(f"  - شناسه سیگنال '{signal_id}' به جدول 'processed_signal_ids' اضافه شد.")
                    
                    updated_symbols += 1

                except (KeyError, ValueError) as e:
                    log(f"  - ⚠️ هشدار: فرمت سیگنال غیرمنتظره است. رد شدن... خطا: {e}")

            conn.commit()
            if updated_symbols > 0:
                log(f"✅ پردازش با موفقیت تمام شد. {updated_symbols} نماد به‌روزرسانی شدند.")
        else:
            log(f"⚠️ درخواست به API ناموفق بود. کد وضعیت: {response.status_code}")

    except requests.exceptions.RequestException as e:
        log(f"❌ خطای شبکه هنگام اتصال به API. خطا: {e}")
    except mysql.connector.Error as e:
        log(f"❌ خطای دیتابیس در حین پردازش سیگنال. خطا: {e}")
    except Exception as e:
        log(f"❌ یک خطای پیش‌بینی نشده در Listener رخ داد. خطا: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            log("اتصال به دیتابیس بسته شد.")

if __name__ == "__main__":
    log("🚀 ربات شنونده سیگنال (Listener) در حال شروع به کار...")
    send_message("<b>🚀 Listener Bot Started (Robust Version)</b>")
    
    # اجرای اولیه راه‌اندازی دیتابیس
    db_conn = create_db_connection()
    if db_conn:
        if setup_database(db_conn):
             db_conn.close()
             while True:
                try:
                    fetch_and_store_signals()
                    wait_interval = config.POLL_INTERVAL if hasattr(config, 'POLL_INTERVAL') else 10
                    log(f"--- چرخه تمام شد. انتظار برای {wait_interval} ثانیه... ---")
                    time.sleep(wait_interval)
                except KeyboardInterrupt:
                    log("🛑 ربات شنونده با دستور کاربر متوقف شد.")
                    send_message("<b>🛑 Listener Bot Stopped Manually</b>")
                    break
        else:
            db_conn.close()
            log("❌ ربات به دلیل عدم موفقیت در راه‌اندازی دیتابیس، متوقف شد.")
    else:
        log("❌ ربات به دلیل عدم اتصال به دیتابیس، متوقف شد.")
