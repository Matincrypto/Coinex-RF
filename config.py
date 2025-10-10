# config.py

# --- CoinEx API Credentials ---
COINEX_ACCESS_ID = "00866D8CC9B04F2BA01901D93544B3A2"
COINEX_SECRET_KEY = "9C186ADBD701CF8311E5F1E0EB8A6100EC36EC32F5D6B8DE"

# --- Trade Settings ---
# The amount in USDT to use as margin for each trade.
USDT_AMOUNT = 10
# The leverage to be used for all trades.
LEVERAGE = 5

# --- Signal API Settings ---
# The URL from which to fetch trading signals.
API_URL = "http://103.75.198.172:8080/signals"

# --- MySQL Database Settings ---
# Replace these with your actual MySQL database credentials.
# --- MySQL Database Settings ---
DB_HOST = "localhost"
DB_USER = "bot_user"                 # <-- نام کاربری موجود شما
DB_PASSWORD = "YourStrongPassword123!" # <-- رمز عبوری که برای این کاربر دارید
DB_NAME = "coinex_rf_analysis_db"      # <-- نام دیتابیس جدید (تغییر نمی‌کند)
DB_PORT = 3306
