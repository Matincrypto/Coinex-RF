# shared_utils.py

import mysql.connector
from datetime import datetime, timezone
import config

def log(message):
    """یک پیام را با فرمت زمانی استاندارد UTC و به زبان فارسی چاپ می‌کند."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp} UTC] {message}")

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
            return conn
    except mysql.connector.Error as e:
        log(f"❌ اتصال به دیتابیس MySQL ناموفق بود. خطا: {e}")
        return None

def setup_database(conn):
    """
    جداول مورد نیاز برنامه را در صورت عدم وجود، در دیتابیس ایجاد می‌کند.
    این تابع باید توسط هر دو اسکریپت در ابتدای کار فراخوانی شود.
    """
    try:
        cursor = conn.cursor()
        log("-> در حال بررسی ساختار دیتابیس و جداول...")
        
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
        log("   - جدول 'signals' آماده است.")

        # جدولی برای نگهداری تمام signal_id های دیده شده
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_signal_ids (
                signal_id VARCHAR(50) PRIMARY KEY
            )
        ''')
        log("   - جدول 'processed_signal_ids' آماده است.")
        
        conn.commit()
        log("✅ ساختار دیتابیس با موفقیت بررسی و آماده شد.")
        return True
    except mysql.connector.Error as e:
        log(f"❌ خطا در هنگام آماده‌سازی دیتابیس. خطا: {e}")
        return False
