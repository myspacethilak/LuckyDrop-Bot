import os
import pytz

# Load environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID', 0))
MONGO_URI = os.getenv('MONGO_URI')
CASHFREE_APP_ID = os.getenv('CASHFREE_APP_ID')
CASHFREE_SECRET_KEY = os.getenv('CASHFREE_SECRET_KEY')
MAIN_CHANNEL_ID = int(os.getenv('MAIN_CHANNEL_ID', 0))
ADMIN_SECRET_CODE = os.getenv('ADMIN_SECRET_CODE')

# Define centralized timezone constants
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')
UTC_TIMEZONE = pytz.utc

# Pot-related constants
DEFAULT_POT_START_HOUR = 17 # 5 PM IST
DEFAULT_POT_END_HOUR = 19   # 7 PM IST
DEFAULT_MAX_USERS = 30
DEFAULT_TICKET_PRICE = 50.0
REVEAL_DELAY_MINUTES = 10