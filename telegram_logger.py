import requests

# Your Telegram Bot credentials
BOT_TOKEN = "7435237309:AAEAXXkce1VU8Wk-NqxX1v6VKnSMaydbErs"
CHAT_ID = "-1002964082215"

def send_message(message):
    """
    Sends a message to the specified Telegram chat.
    Uses HTML format for rich text.
    """
    # Telegram API URL
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    # Payload with chat_id, message text, and parse_mode
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'  # Using HTML for bold, code, etc.
    }

    try:
        # Send the request
        response = requests.post(url, json=payload, timeout=10)
        # Optional: Check if the message was sent successfully
        if response.status_code != 200:
            # If sending fails, print the error to the console
            print(f"Error sending Telegram message: {response.text}")
    except Exception as e:
        # If there's a network error or other issue, print it to the console
        print(f"Failed to send Telegram message: {e}")

# You can test this file directly by running `python telegram_logger.py`
if __name__ == '__main__':
    print("Sending a test message to Telegram...")
    send_message("<b>✅ Test Message</b>\nThis is a test message from the trading bot logger.")
    print("Test message sent.")