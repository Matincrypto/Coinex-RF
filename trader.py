# trader.py (نسخه جدید با حلقه تکرار هوشمند برای فاندینگ)

import ccxt
import time
from datetime import datetime, timezone
import config
from telegram_logger import send_message
from shared_utils import log, create_db_connection, setup_database

MAX_SIGNAL_AGE_MINUTES = 5
RETRY_INTERVAL_SECONDS = 30 # فاصله زمانی برای تلاش مجدد هنگام تسویه فاندینگ

def get_active_positions(exchange):
    """
    پوزیشن‌های باز را از صرافی دریافت می‌کند.
    اگر صرافی در حال تسویه فاندینگ باشد، تا زمان در دسترس شدن، منتظر می‌ماند و دوباره تلاش می‌کند.
    """
    while True: # حلقه تکرار هوشمند
        try:
            log("۱. در حال دریافت پوزیشن‌های باز از صرافی...")
            params = {'type': 'swap'}
            all_positions = exchange.fetch_positions(None, params)
            
            open_positions = {p['symbol']: p for p in all_positions if float(p.get('contracts', 0)) > 0}
            
            if open_positions:
                log(f"   -> {len(open_positions)} پوزیشن باز پیدا شد: {list(open_positions.keys())}")
            else:
                log("   -> هیچ پوزیشن بازی در صرافی وجود ندارد.")
                
            return open_positions # <-- اگر موفق بود، از حلقه خارج شو و نتیجه را برگردان

        except ccxt.BaseError as e:
            # بررسی هوشمندانه متن خطا
            if 'funding fee settlement' in str(e):
                log(f"🟡 هشدار صرافی: سرویس در حین تسویه کارمزد فاندینگ در دسترس نیست.")
                log(f"   -> ربات متوقف نمی‌شود. {RETRY_INTERVAL_SECONDS} ثانیه دیگر دوباره تلاش خواهد شد...")
                time.sleep(RETRY_INTERVAL_SECONDS)
                # حلقه ادامه پیدا می‌کند و دوباره تلاش می‌کند
            else:
                # اگر خطا مربوط به فاندینگ نبود، یک خطای جدی است
                log(f"❌ خطای جدی در ارتباط با صرافی: {e}")
                send_message(f"<b>❌ خطای جدی در دریافت پوزیشن</b>\n\n<b>خطا:</b>\n<code>{e}</code>")
                return None # <-- برای خطاهای دیگر، از حلقه خارج شو و خطا را برگردان
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
        # ۱. دریافت پوزیشن‌های فعال (با منطق جدید)
        open_positions = get_active_positions(exchange)
        
        # اگر get_active_positions یک خطای جدی برگرداند، این چرخه را رد کن
        if open_positions is None:
            log("   -> پردازش این چرخه به دلیل خطای جدی در دریافت پوزیشن متوقف شد.")
            return

        # ۲. اتصال به دیتابیس و دریافت سیگنال جدید
        conn = create_db_connection()
        if not conn:
            log("پردازش به دلیل عدم اتصال به دیتابیس متوقف شد.")
            return

        signal = get_new_signal(conn)
        if not signal:
            log("۲. سیگنال جدیدی برای پردازش در دیتابیس وجود ندارد.")
            return
        
        log(f"۲. در حال پردازش سیگنال برای نماد {signal['symbol']}...")

        # ۳. بررسی تاریخ انقضای سیگنال (سیگنال سوخته)
        signal_age = datetime.now(timezone.utc) - datetime.fromtimestamp(signal['timestamp'], tz=timezone.utc)
        if signal_age.total_seconds() > (MAX_SIGNAL_AGE_MINUTES * 60):
            log(f"🟡 سیگنال نادیده گرفته شد (سوخته). عمر سیگنال: {signal_age}")
            update_signal_status(conn, signal['symbol'], 'processed_burnt')
            return

        # ۴. منطق اصلی معامله
        symbol, order_side = signal['symbol'], signal['side'].lower()
        price = signal['price']
        
        if symbol in open_positions:
            existing_pos = open_positions[symbol]
            log(f"   -> یک پوزیشن '{existing_pos['side'].upper()}' برای {symbol} از قبل باز است.")
            
            if existing_pos['side'] != order_side:
                log(f"   -> سیگنال معکوس شناسایی شد! در حال بستن پوزیشن فعلی...")
                try:
                    close_side = 'sell' if existing_pos['side'] == 'buy' else 'buy'
                    closing_order = exchange.create_order(symbol, 'limit', close_side, existing_pos['contracts'], price, {'reduceOnly': True})
                    log(f"   ✅ سفارش بستن پوزیشن با موفقیت ثبت شد. شناسه: {closing_order['id']}")
                    send_message(f"<b>⏳ بستن پوزیشن (سیگنال معکوس)</b>\n\n<b>نماد:</b> {symbol}\n<b>جهت:</b> {existing_pos['side'].upper()}")
                except Exception as e:
                    log(f"   ❌ خطا در بستن پوزیشن معکوس. خطا: {e}")
                    send_message(f"<b>❌ خطا در بستن پوزیشن</b>\n\n<b>نماد:</b> {symbol}\n<b>خطا:</b>\n<code>{e}</code>")
                    update_signal_status(conn, symbol, 'processed_error')
            else:
                log("   -> سیگنال هم‌جهت با پوزیشن فعلی است. نادیده گرفته شد.")
                update_signal_status(conn, symbol, 'processed_duplicate')
        else:
            log("   -> پوزیشن بازی برای این نماد وجود ندارد. در حال باز کردن پوزیشن جدید...")
            try:
                total_value = config.USDT_AMOUNT * config.LEVERAGE
                amount_to_trade = total_value / price
                log(f"   -> محاسبه مقادیر: مارجین=${config.USDT_AMOUNT}, لوریج={config.LEVERAGE}x, مقدار ارز={amount_to_trade:.6f}")
                
                new_order = exchange.create_order(symbol, 'limit', order_side, amount_to_trade, price)
                log(f"   ✅ پوزیشن جدید با موفقیت باز شد! شناسه سفارش: {new_order['id']}")
                send_message(f"<b>{'📈' if order_side == 'buy' else '📉'} باز شدن پوزیشن جدید</b>\n\n"
                             f"<b>نماد:</b> {symbol}\n<b>جهت:</b> {order_side.upper()}\n<b>قیمت:</b> {price}\n"
                             f"<b>مقدار:</b> {amount_to_trade:.6f}")
                update_signal_status(conn, symbol, 'processed')
            except Exception as e:
                log(f"   ❌ خطا در باز کردن پوزیشن جدید. خطا: {e}")
                send_message(f"<b>❌ خطا در باز کردن پوزیشن</b>\n\n<b>نماد:</b> {symbol}\n<b>خطا:</b>\n<code>{e}</code>")
                update_signal_status(conn, symbol, 'processed_error')

    except Exception as e:
        log(f"❌ یک خطای پیش‌بینی نشده در چرخه اصلی Trader رخ داد. خطا: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            log("اتصال به دیتابیس بسته شد.")

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
