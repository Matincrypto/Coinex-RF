# test_order_with_leverage.py
import ccxt
import config

# --- Test Order Details ---
TEST_SYMBOL = 'BTCUSDT'
TEST_PRICE = 103000.0
TEST_SIDE = 'buy'
TEST_LEVERAGE = 20  # The desired leverage

def place_test_order_with_leverage():
    """
    Connects to CoinEx, sets leverage for the market, and then places a single limit order.
    """
    print("--- Starting Test Script with Leverage ---")
    print(f"Target Symbol: {TEST_SYMBOL} | Price: ${TEST_PRICE} | Leverage: {TEST_LEVERAGE}x")

    try:
        # 1. Initialize the connection
        exchange = ccxt.coinex({
            'apiKey': config.COINEX_ACCESS_ID,
            'secret': config.COINEX_SECRET_KEY,
            'options': {'defaultType': 'swap'},
        })
        print("✅ Successfully connected to CoinEx.")

        # --- NEW: Set Leverage ---
        print(f"-> Setting leverage for {TEST_SYMBOL} to {TEST_LEVERAGE}x...")
        # For CoinEx, you must specify the market type in the params
        params = {'market_type': 'FUTURES'}
        exchange.set_leverage(TEST_LEVERAGE, TEST_SYMBOL, params)
        print(f"✅ Leverage successfully set to {TEST_LEVERAGE}x.")
        # -------------------------

        # 2. Calculate the order amount
        # Note: The position value will be (USDT_AMOUNT * LEVERAGE)
        amount_to_trade = (config.USDT_AMOUNT * TEST_LEVERAGE) / TEST_PRICE
        print(f"-> Calculated amount: {amount_to_trade:.8f} BTC for a position worth approx. ${config.USDT_AMOUNT * TEST_LEVERAGE}")

        # 3. Place the limit order
        print("-> Sending order to the exchange...")
        order = exchange.create_order(
            TEST_SYMBOL,
            'limit',
            TEST_SIDE,
            amount_to_trade,
            TEST_PRICE,
            params
        )

        print("\n🎉 SUCCESS! Order placed successfully.")
        print("--- Order Details ---")
        print(f"  ID: {order.get('id')}")
        print(f"  Symbol: {order.get('symbol')}")
        print(f"  Amount: {order.get('amount')}")
        print(f"  Price: {order.get('price')}")
        print("---------------------")
        print("Please check your CoinEx account to confirm the open order and leverage setting.")

    except ccxt.ExchangeError as e:
        print(f"\n❌ EXCHANGE ERROR: {e}")
    except ccxt.NetworkError as e:
        print(f"\n❌ NETWORK ERROR: {e}")
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")

if __name__ == "__main__":
    place_test_order_with_leverage()