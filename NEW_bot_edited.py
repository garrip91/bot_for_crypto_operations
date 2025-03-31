import asyncio
import ccxt.async_support as ccxt
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command, CommandStart
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
import time
import datetime
import logging
import sqlite3
import re
import inspect
from datetime import datetime, timedelta
from collections import defaultdict
import traceback
import functools

from src import secrets

#==========================================================================================================================
#======================================================= INITIALIZATION ====================================================
#==========================================================================================================================

#----------------------------------------------- BINANCE SETUP -------------------------------------------------------------

# Binance setup with async support
exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'  # Switch to USDT-M Futures markets
    }
})

#-------------------------------------------- PRICE BOT INITIALIZATION -----------------------------------------------------

prices = {}  # Dictionary to store the latest prices for multiple pairs
prices_cooldown = {}  # Dictionary to manage cooldown states for each trading pair

PRICE_TELEGRAM_TOKEN = secrets.PRICE_TELEGRAM_TOKEN
DEBUG_BOT_TOKEN = secrets.DEBUG_BOT_TOKEN

all_pairs = []   # Store all available pairs
ignored_pairs = set() # Store ignored pairs from the ban table

DEBUG_CHAT_ID = secrets.DEBUG_CHAT_ID

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

WHITELIST_DB_PATH = "/var/www/site/payment/whitelist.db"
BAN_PAIRS_DB_PATH = "/var/www/site/payment/ban_pairs.db"

#---------------------------------------- RATE LIMITING AND MESSAGE QUEUE --------------------------------------------------

# Constants for global rate limiting
GLOBAL_MESSAGES_PER_SECOND = 30  # Global limit of messages per second

# Constants for per-user rate limiting
USER_MESSAGES_PER_MINUTE = 15    # Adjusted per-user limit of messages per minute

# Global variables for message handling
message_queue = []            # List to hold messages to be sent
user_message_counts = {}      # Tracks messages sent per user per minute
total_messages_queued = 0     # Total messages queued in the current minute
total_messages_sent = 0       # Total messages sent in the current minute
blocked_users = set()         # Set of users who have been blocked due to rate limits
last_message_time = {}        # Tracks the last message time per chat for rate limiting
user_flood_timeout = {}       # Tracks flood wait times per user
blocked_user_ids_forbidden = set()

prices_lock = asyncio.Lock()  # Global lock for prices

#---------------------------------------- ALERT LIMIT --------------------------------------------------

notification_counters = defaultdict(lambda: defaultdict(int))
last_counter_reset_date = datetime.now().date()

#------------------------------------------- ERROR HANDLING VARIABLES ------------------------------------------------------

fetch_errors = []  # Global list to collect errors
last_error_message_time = 0
ERROR_MESSAGE_INTERVAL = 60  # Minimum interval in seconds between error messages

# Initialize bots
price_bot = Bot(token=PRICE_TELEGRAM_TOKEN)
debug_bot = Bot(token=DEBUG_BOT_TOKEN)

# Initialize dispatchers
price_dp = Dispatcher()
debug_dp = Dispatcher()

# Initialize routers
price_router = Router()
debug_router = Router()

# Bot data storage (replacement for context.bot_data)
bot_data = {}
user_data = {}

#=========================================================================================================================================================================
#==========================================================ERROR HANDLERS========================================================================================
#=========================================================================================================================================================================

def telegram_error_handler(handler):
    """
    Декоратор для обработки ошибок в обработчиках Telegram.
    Адаптирован для работы с aiogram 3.10.0
    """
    @functools.wraps(handler)
    async def wrapper(*args, **kwargs):
        try:
            # Удаляем аргумент dispatcher, если он присутствует
            kwargs_copy = kwargs.copy()
            if 'dispatcher' in kwargs_copy:
                del kwargs_copy['dispatcher']
            
            # Проверяем подпись функции и удаляем лишние аргументы
            sig = inspect.signature(handler)
            param_names = set(p.name for p in sig.parameters.values())
            
            # Оставляем только те аргументы, которые ожидает функция
            filtered_kwargs = {k: v for k, v in kwargs_copy.items() if k in param_names}
            
            # Вызываем оригинальный обработчик
            return await handler(*args, **filtered_kwargs)
        except Exception as e:
            # Пытаемся получить объект сообщения
            message = None
            for arg in args:
                if isinstance(arg, Message):
                    message = arg
                    break
            
            # Получаем chat_id из объекта сообщения
            chat_id = "Unknown"
            if message and hasattr(message, 'chat') and message.chat:
                chat_id = message.chat.id
            
            # Формируем сообщение об ошибке
            error_message = f"Error in {handler.__name__} with chat_id {chat_id}: {e}"
            print(error_message)
            traceback.print_exc()  # Печатаем полный стек ошибки
            
            # Отправляем сообщение об ошибке в отладочный чат
            try:
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
            except Exception as debug_error:
                print(f"Failed to send error message: {debug_error}")
            
            return None
    
    return wrapper


async def async_error_handler(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except Exception as e:
        # Extract message if it's in args
        message = next((arg for arg in args if isinstance(arg, Message)), None)
        chat_id = message.chat.id if message else "Unknown"
        error_message = f"Error in {func.__name__} with chat_id {chat_id}: {e}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


def global_timeout_retry(retries=3, delay=5):
    """Decorator to retry a function in case of a TimedOut error."""
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            # Handle async functions
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                # Удаляем аргумент dispatcher, если он присутствует
                kwargs_copy = kwargs.copy()
                if 'dispatcher' in kwargs_copy:
                    del kwargs_copy['dispatcher']
                
                # Проверяем подпись функции и удаляем лишние аргументы
                sig = inspect.signature(func)
                param_names = set(p.name for p in sig.parameters.values())
                
                # Оставляем только те аргументы, которые ожидает функция
                filtered_kwargs = {k: v for k, v in kwargs_copy.items() if k in param_names}
                
                for attempt in range(retries):
                    try:
                        return await func(*args, **filtered_kwargs)  # Try running the coroutine with filtered kwargs
                    except Exception as e:
                        if 'TimedOut' in str(e):
                            # Handle Telegram TimedOut errors
                            print(f"Telegram TimedOut error in async {func.__name__}: {e}. Retrying in {delay} seconds...")
                            await asyncio.sleep(delay)  # Wait before retrying
                        else:
                            # Catch any other unexpected errors
                            print(f"Unexpected error in async {func.__name__}: {e}. No retry.")
                            break  # Stop retrying for non-TimedOut errors
            return async_wrapper
        else:
            # Handle synchronous functions
            def sync_wrapper(*args, **kwargs):
                for attempt in range(retries):
                    try:
                        return func(*args, **kwargs)  # Try running the function
                    except Exception as e:
                        if 'TimedOut' in str(e):
                            # Handle Telegram TimedOut errors
                            print(f"Telegram TimedOut error in {func.__name__}: {e}. Retrying in {delay} seconds...")
                            time.sleep(delay)  # Wait before retrying
                        else:
                            # Catch any other unexpected errors
                            print(f"Unexpected error in {func.__name__}: {e}. No retry.")
                            break  # Stop retrying for non-TimedOut errors
            return sync_wrapper
    return decorator

#======================================================================================================================
#=========================================FETCHING AND COMPARING PRICES================================================
#======================================================================================================================

def get_ignored_pairs():
    db_path = BAN_PAIRS_DB_PATH
    ignored_pairs = []
    
    try:
        # Connect to the SQLite database
        db = sqlite3.connect(db_path)
        cursor = db.cursor()

        # Fetch all rows from the ban table
        cursor.execute('SELECT pair FROM ban')
        rows = cursor.fetchall()

        # Extract the pair names from the rows
        ignored_pairs = [row[0] for row in rows]

        # Close the connection
        db.close()

    except sqlite3.Error as e:
        error_message = f"Database error in get_ignored_pairs: {e}"
        print(error_message)
        asyncio.create_task(debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message))
    
    return ignored_pairs

@global_timeout_retry(retries=3, delay=5)
async def price_fetch_initial_prices():
    global all_pairs, ignored_pairs

    start_time = datetime.now().strftime("%H:%M:%S")

    # 1) Fetch the list of ignored pairs from the SQLite database
    ignored_pairs = get_ignored_pairs()

    # 2) Load all available USDT-M PERPETUAL markets
    markets = await exchange.load_markets()
    all_pairs = [
        pair for pair, data in markets.items()
        if data.get('info', {}).get('contractType') == 'PERPETUAL' and data.get('quote') == 'USDT'
    ]

    # 3) Exclude ignored pairs
    pairs = [pair for pair in all_pairs if pair not in ignored_pairs]
    ignored_pairs_count = len([pair for pair in all_pairs if pair in ignored_pairs])

    # 4) **Fetch all futures tickers at once**
    all_tickers = await exchange.fetch_tickers(pairs)

    fetched = 0
    skipped = 0

    # 5) Extract and store prices
    for pair in pairs:
        ticker = all_tickers.get(pair)
        if not ticker:
            # If nothing is returned for this pair, skip
            skipped += 1
            continue

        latest_price = ticker.get('last')
        if latest_price is not None:
            prices[pair] = [latest_price]
            fetched += 1
        else:
            skipped += 1

    end_time = datetime.now().strftime("%H:%M:%S")

    summary_message = (
        f"{start_time} -> {end_time} - Initial prices collected\n"
        f"{fetched} Fetched. {skipped} Skipped. {ignored_pairs_count} Ignored"
    )
    print(summary_message)
    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)

    # Return only the successfully fetched pairs
    return [pair for pair in pairs if pair in prices]


@global_timeout_retry(retries=3, delay=5)
async def price_fetch_and_compare_prices():
    try:
        # 1) Get all tracked pairs
        tracked_pairs = list(prices.keys())
        if not tracked_pairs:
            return 0

        # 2) Bulk-fetch tickers for all tracked pairs
        all_tickers = await exchange.fetch_tickers(tracked_pairs)

        fetched_count = 0

        # 3) Update each pair's price
        async with prices_lock:
            for pair in tracked_pairs:
                ticker_data = all_tickers.get(pair, {})
                new_price = ticker_data.get('last')
                if new_price is not None:
                    if pair in prices:
                        prices[pair].insert(0, new_price)
                        if len(prices[pair]) > 30:
                            prices[pair].pop()
                    else:
                        prices[pair] = [new_price]
                    fetched_count += 1

        return fetched_count

    except Exception as e:
        error_details = (
            f"Error fetching bulk prices:\n"
            f"{e.__class__.__name__}: {str(e)}\n"
            f"{traceback.format_exc()}"
        )
        print(error_details)
        fetch_errors.append(error_details)
        return 0


@global_timeout_retry(retries=3, delay=5)
async def reinitialize_pairs():
    global all_pairs, prices, ignored_pairs

    start_time = datetime.now().strftime("%H:%M:%S")

    try:
        # 1) Fetch ignored pairs from the database
        ignored_pairs = get_ignored_pairs()

        # 2) Fetch all available futures markets
        markets = await exchange.load_markets()
        current_all_pairs = [
            pair for pair, data in markets.items()
            if data.get('info', {}).get('contractType') == 'PERPETUAL' and data.get('quote') == 'USDT'
        ]

        # 3) Remove stale entries
        async with prices_lock:
            for pair in list(prices.keys()):
                if pair in ignored_pairs or not prices[pair]:
                    del prices[pair]

        # 4) Bulk-fetch tickers once
        all_tickers = await exchange.fetch_tickers(current_all_pairs)

        fetched, skipped, ignored_pairs_count = 0, 0, 0

        # 5) Process the ticker data
        for pair in current_all_pairs:
            if pair in ignored_pairs:
                ignored_pairs_count += 1
                continue

            ticker = all_tickers.get(pair, {})
            latest_price = ticker.get('last')
            if latest_price is not None:
                async with prices_lock:
                    if pair in prices:
                        prices[pair].insert(0, latest_price)
                        if len(prices[pair]) > 30:
                            prices[pair].pop()
                    else:
                        prices[pair] = [latest_price]
                fetched += 1
            else:
                skipped += 1

        # 6) Update global pairs list
        all_pairs = current_all_pairs

        # 7) Synchronize prices_cooldown dictionary
        async with prices_lock:
            for pair in list(prices_cooldown.keys()):
                if pair not in prices:
                    del prices_cooldown[pair]

        end_time = datetime.now().strftime("%H:%M:%S")
        summary_message = (
            f"{start_time} -> {end_time} - Re-initialization done\n"
            f"{fetched} Fetched. {skipped} Skipped. {ignored_pairs_count} Ignored"
        )
        print(summary_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)

    except Exception as e:
        error_message = f"Reinitialization error: {str(e)}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


#==================================================================================================================
#==================================================SENDING ALERTS==================================================
#==================================================================================================================

def is_user_whitelisted_and_active(chat_id):
    db_path = WHITELIST_DB_PATH
    db = sqlite3.connect(db_path)
    cursor = db.cursor()

    # Query to check if the user exists and is active in the whitelist table
    cursor.execute('SELECT 1 FROM whitelist WHERE TelegramID = ? AND Active = 1', (chat_id,))
    result = cursor.fetchone()

    # Close the database connection
    db.close()

    # Return True if the user is found and active, otherwise False
    return result is not None


@global_timeout_retry(retries=3, delay=5)
async def price_send_alert(pair, change_percent, old_price, new_price, price_list, condition_type, settings, chat_id):
    global message_queue, total_messages_queued
    global notification_counters

    # Assign emojis and signal names based on condition type
    if condition_type == 'Short':
        emoji = '🟢'
        signal_name = 'Pump Signal'
        period = f"{settings['pump_index']} min"
    elif condition_type == 'Dump':
        emoji = '🔴'
        signal_name = 'Dump Signal'
        period = f"{settings['dump_index']} min"
    else:
        emoji = 'ℹ️'
        signal_name = condition_type
        period = ''

    raw_symbol = pair.replace(':USDT', '').replace('/', '')
    url_symbol = raw_symbol
    pair_display = raw_symbol.replace('USDT', '')

    # Build the TradingView link with no extra path (no KucqLzAO)
    url = f"https://www.tradingview.com/chart/?symbol=BINANCE%3A{url_symbol}.P"
    hyperlink = f'<a href="{url}">{pair_display}</a>'

    # Format prices
    formatted_old_price = f"{old_price:.8f}".rstrip('0').rstrip('.')
    formatted_new_price = f"{new_price:.8f}".rstrip('0').rstrip('\.')

    # Alert numbering
    alert_number = notification_counters[chat_id][pair] + 1

    # Compose the alert message
    message = (
        f"{emoji} <b>{hyperlink}</b> | <i>{period}</i> | {signal_name} {emoji}\n"
        f"Change: <b>{abs(change_percent):.2f}%</b>\n"
        f"{formatted_old_price} -> <b>{formatted_new_price}</b>\n"
        f"🔇 Alert Number: <b>{alert_number}</b>"
    )

    # Queue the message
    message_queue.append((chat_id, message))
    total_messages_queued += 1


def extract_retry_after(error_message):
    # Extract the retry time from the error message
    match = re.search(r"Retry in (\d+) seconds", error_message)
    if match:
        return int(match.group(1))
    return 30  # Default to 30 seconds if no specific time is found


async def price_check_and_send_notifications():
    global notification_counters, last_counter_reset_date

    current_date = datetime.now().date()
    if current_date != last_counter_reset_date:
        # Reset counters if the date has changed
        notification_counters = defaultdict(lambda: defaultdict(int))
        last_counter_reset_date = current_date

    async with prices_lock:
        for pair, price_list in prices.items():
            # Initialize the cooldown dictionary for the pair if not exists
            if pair not in prices_cooldown:
                prices_cooldown[pair] = {}

            if len(price_list) == 0:
                continue  # Skip if no price data

            new_price = price_list[0]

            for chat_id, settings in bot_data.items():
                # Check if user is whitelisted and active
                if not is_user_whitelisted_and_active(chat_id):
                    continue

                # Check alert limit per pair
                alert_limit = settings.get('alert_limit', 20)
                notifications_sent = notification_counters[chat_id][pair]
                if alert_limit is not None and notifications_sent >= alert_limit:
                    continue

                # Initialize cooldown for this chat_id if not exists
                if chat_id not in prices_cooldown[pair]:
                    # Only Pump (Short) and Dump conditions now
                    prices_cooldown[pair][chat_id] = {'Short': 0, 'Dump': 0}

                # Decrement cooldown counters
                for condition in ['Short', 'Dump']:
                    if prices_cooldown[pair][chat_id][condition] > 0:
                        prices_cooldown[pair][chat_id][condition] -= 1

                # Pump condition (formerly pump)
                pump_index = settings['pump_index']
                pump_threshold = settings['pump_threshold']

                if len(price_list) > pump_index and prices_cooldown[pair][chat_id]['Short'] == 0:
                    old_price_pump = price_list[pump_index]
                    change_percent_pump = (new_price - old_price_pump) / old_price_pump * 100
                    if change_percent_pump >= pump_threshold:
                        # Send pump (pump) alert
                        await price_send_alert(pair, change_percent_pump, old_price_pump, new_price, price_list, 'Short', settings, chat_id)
                        prices_cooldown[pair][chat_id]['Short'] = pump_index
                        notification_counters[chat_id][pair] += 1
                        continue  # If pump triggered, skip dump check for this iteration

                # Dump condition
                d_index = settings['dump_index']
                d_threshold = settings['dump_threshold']

                if len(price_list) > d_index and prices_cooldown[pair][chat_id]['Dump'] == 0:
                    old_price_d = price_list[d_index]
                    change_percent_d = (new_price - old_price_d) / old_price_d * 100
                    if change_percent_d <= -d_threshold:
                        # Send dump alert
                        await price_send_alert(pair, change_percent_d, old_price_d, new_price, price_list, 'Dump', settings, chat_id)
                        prices_cooldown[pair][chat_id]['Dump'] = d_index
                        notification_counters[chat_id][pair] += 1


async def send_message(chat_id, message):
    global total_messages_sent, last_message_time, user_flood_timeout
    global blocked_user_ids_forbidden  # The set() we defined to accumulate blocked-user IDs

    # Check if the user is currently in flood wait
    if chat_id in user_flood_timeout:
        if time.time() < user_flood_timeout[chat_id]:
            print(f"Skipping message to {chat_id} due to flood wait.")
            return
        else:
            del user_flood_timeout[chat_id]

    current_time = time.time()
    last_time = last_message_time.get(chat_id, 0)
    time_since_last_message = current_time - last_time

    if time_since_last_message < 1:
        time_to_wait = 1 - time_since_last_message
        await asyncio.sleep(time_to_wait)

    try:
        await price_bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
        total_messages_sent += 1
        last_message_time[chat_id] = time.time()

    except Exception as e:
        error_str = str(e)
        if 'retry_after' in error_str.lower():
            # Handle retry after in exception message
            match = re.search(r'retry_after=(\d+)', error_str)
            retry_after = int(match.group(1)) if match else 30
            print(f"Flood control exceeded for chat_id {chat_id}. Retry in {retry_after} seconds.")
            user_flood_timeout[chat_id] = time.time() + retry_after
            message_queue.append((chat_id, message))
        elif "bot was blocked by the user" in error_str.lower():
            # The user blocked the bot
            blocked_user_ids_forbidden.add(chat_id)
        else:
            # Some other error
            error_message = f"Error sending to {chat_id}: {error_str}"
            print(error_message)
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


async def process_message_queue():
    global message_queue, user_message_counts, blocked_users, user_flood_timeout
    global blocked_user_ids_forbidden  # The set() we defined

    while message_queue:
        chat_id, message = message_queue.pop(0)

        if chat_id in user_flood_timeout:
            if time.time() < user_flood_timeout[chat_id]:
                continue
            else:
                del user_flood_timeout[chat_id]

        if user_message_counts.get(chat_id, 0) < USER_MESSAGES_PER_MINUTE:
            user_message_counts[chat_id] = user_message_counts.get(chat_id, 0) + 1
            await send_message(chat_id, message)
        else:
            blocked_users.add(chat_id)
            # Optionally discard or defer the message

    # At the end, send one summary message if any users blocked the bot
    if blocked_user_ids_forbidden:
        summary_message = (
            "The following users have blocked the bot:\n"
            + ", ".join(str(uid) for uid in blocked_user_ids_forbidden)
        )
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)
        blocked_user_ids_forbidden.clear()


#=========================================================================================================================================================
#=======================================================================BUTTONS===========================================================================
#=========================================================================================================================================================

def load_user_data():
    try:
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()
        cursor.execute('SELECT TelegramID, Pindex, Ppercent, Dindex, Dpercent, Filter FROM whitelist WHERE Active = 1 OR Test = 1')
        rows = cursor.fetchall()
        for row in rows:
            telegram_id = int(row[0])
            p_index = row[1]
            p_percent = row[2]
            d_index = row[3]
            d_percent = row[4]
            alert_limit = row[5] if row[5] is not None else 100  # Default to 100 if NULL

            # Updated dictionary without long conditions
            bot_data[telegram_id] = {
                'pump_index': p_index,
                'pump_threshold': p_percent,
                'dump_index': d_index,
                'dump_threshold': d_percent,
                'alert_limit': alert_limit
            }
        db.close()
        print(f"Loaded user data for {len(rows)} users.")
    except sqlite3.Error as e:
        error_message = f"Database error in load_user_data: {e}"
        print(error_message)
        asyncio.create_task(debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message))


@price_router.message(CommandStart())
async def price_start(message: Message):
    try:
        chat_id = message.chat.id
        user = message.from_user
        username = user.username or ''
        
        # Parse arguments from command
        args = message.text.split()[1:] if message.text.split() else []
        referral_code = args[0] if args else None

        try:
            # Connect to the SQLite database
            db = sqlite3.connect(WHITELIST_DB_PATH)
            cursor = db.cursor()

            # Check if the user is already in the whitelist
            cursor.execute('SELECT Active, Test, StartDate, EndDate, Pindex, Ppercent, Dindex, Dpercent FROM whitelist WHERE TelegramID = ?', (chat_id,))
            result = cursor.fetchone()

            if result:
                # User exists in the database
                active, test, start_date_db, end_date_db, p_index, p_percent, d_index, d_percent = result
                is_new_user = False
                start_date = start_date_db
                end_date = end_date_db
            else:
                # User is new, insert into the database
                trial_days = 3  # Default trial period is 2 days

                if referral_code:
                    # Check if referral_code exists in 'referral' table
                    cursor.execute('SELECT referral_link FROM referral WHERE referral_link = ?', (referral_code,))
                    referral_result = cursor.fetchone()
                    if referral_result:
                        # Referral code is valid
                        trial_days = 7
                    else:
                        # Invalid referral code
                        await message.reply("Invalid referral code provided. You have been granted a 2-day trial period.")

                start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                end_date = (datetime.now() + timedelta(days=trial_days)).strftime('%Y-%m-%d %H:%M:%S')  
                
                # Default values for Pump (previously pump) and Dump settings
                p_index, p_percent = 3, 5
                d_index, d_percent = 2, 8
                active = 1
                test = 1
                cursor.execute('''
                    INSERT INTO whitelist (TelegramID, Username, Referral, Active, Test, StartDate, EndDate, Pindex, Ppercent, Dindex, Dpercent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (chat_id, username, referral_code, active, test, start_date, end_date, p_index, p_percent, d_index, d_percent))
                db.commit()
                is_new_user = True

            # Store user settings in bot_data
            bot_data[chat_id] = {
                'pump_index': p_index,
                'pump_threshold': p_percent,
                'dump_index': d_index,
                'dump_threshold': d_percent
            }

            # Compose the welcome message
            welcome_message = (
                "Welcome to the Pump Bot!\n\n"
                "⭐️<b>How can this bot help you?</b>⭐️\n"
                "The Pump Bot is designed to keep you ahead of the market. Every second, it scans all market coins to detect price changes in real-time. "
                "Whether prices are surging or dropping, this bot will notify you immediately, so you never miss an opportunity.\n\n"
                "💰<b>How to make money with the Pump Bot?</b>💰\n"
                "The Pump Bot is your ultimate tool for identifying profit-making opportunities in the market."
                "It allows you to act on Pumps, <b> trade corrections, and short squeezes.</b> \n"
                "To maximize your trading success, check out our dedicated channel: https://t.me/opportunity_trading_info \n\n"
                "🤖<b>How to use the bot?</b>🤖\n"
                "You can customize the bot's behavior by pressing the <b>Bot Settings</b> button below and selecting your preferences. "
                "You decide which price changes matter most to you and over what time period. Here are two examples:\n\n"
                "⚡️<b>Example 1:</b>\n"
                "I want to be notified if a coin's price increases by 4% within 2 minutes.\n"
                "In this case, the bot will detect any coin that grows by 4% in that time frame and notify you right away.\n\n"
                "⚡️<b>Example 2:</b>\n"
                "I want to be alerted if a coin drops by 15% over 30 minutes.\n"
                "If such a drop occurs, the bot will instantly notify you, ensuring you're always informed about significant market movements.\n\n"
                "👨‍💻<b>Do you have any questions?</b>👨‍💻\n"
                "We're here to help! For full support, feel free to contact us at <a href='https://t.me/opportunity_trading'>t.me/opportunity_trading</a>."
            )

            # Define the reply keyboard
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Bot Settings"), KeyboardButton(text="Payment Settings")],
                    [KeyboardButton(text="Contact Support")]
                ],
                resize_keyboard=True,
                one_time_keyboard=True
            )

            # Send the welcome message and attach the keyboard
            await message.reply(welcome_message, parse_mode='HTML', reply_markup=keyboard)

            # If the user is new, send the trial message after the welcome message
            if is_new_user:
                trial_message = (
                    f"<b>Your Trial period has started and lasts for {trial_days} days.</b>\n"
                    f"<b>It will expire at {end_date}.</b>\n"
                    "Check more details by pressing <b>Payment Settings -> Check Profile</b>.\n"
                    "<b>Good luck with your trading!</b>"
                )
                await message.reply(trial_message, parse_mode='HTML')

            # DEBUG MESSAGE BELOW
            current_time = datetime.now().strftime("%H:%M:%S")
            debug_message = f"{current_time} New user {chat_id} added or updated"
            print(debug_message)
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)

            # Close the database connection
            db.close()

        except sqlite3.Error as e:
            error_message = f"Database error in price_start: {e}"
            print(error_message)
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
    except Exception as e:
        error_message = f"Error in price_start: {e}"
        print(error_message)
        traceback.print_exc()
        try:
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
        except:
            print("Failed to send error message to debug chat")


@price_router.message(F.text == "Bot Settings")
async def price_show_bot_settings(message: Message):
    try:
        chat_id = message.chat.id

        # Connect to the SQLite database
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()

        # Fetch Active and Test status
        cursor.execute('SELECT Active, Test FROM whitelist WHERE TelegramID = ?', (chat_id,))
        status_check = cursor.fetchone()

        active = int(status_check[0]) if status_check else 0
        test = int(status_check[1]) if status_check else 0

        # Fetch settings for Pump (Pindex/Ppercent) and Dump (Dindex/Dpercent), as well as alert_limit
        cursor.execute('SELECT Pindex, Ppercent, Dindex, Dpercent, Filter FROM whitelist WHERE TelegramID = ?', (chat_id,))
        user_settings = cursor.fetchone()

        if user_settings:
            p_index, p_percent, d_index, d_percent, alert_limit = user_settings
            alert_limit = alert_limit if alert_limit is not None else 100
        else:
            # Default values if no settings found
            p_index, p_percent = 3, 5
            d_index, d_percent = 2, 8
            alert_limit = 100

        # Store user settings in bot_data
        bot_data[chat_id] = {
            'pump_index': p_index,      # now representing Pump
            'pump_threshold': p_percent, 
            'dump_index': d_index,
            'dump_threshold': d_percent,
            'alert_limit': alert_limit
        }

        # First message: How to change settings (no mention of Long now)
        first_message = (
            f"<b>How to change settings:</b>\n"
            f"Press the button for the setting you want to change, then send the desired amount.\n\n"
            f"<i>Signal Periods</i>: 1 to 30 (minutes)\n"
            f"<i>Signal Percentages</i>: 1% to 100%\n"
            f"<i>Alert Limit</i>: 1 to 20 or 'all' for unlimited alerts per pair per day.\n\n"
            f"Examples:\n\n"
            f"- Notify me if a coin's price increases by 10% in 2 minutes:\n"
            f"🟢 Pump Period: 2\n"
            f"➗ Pump Percentage: 10\n\n"
            f"- Notify me if a coin's price drops by 15% in 10 minutes:\n"
            f"🔴 Dump Period: 10\n"
            f"➗ Dump Percentage: 15\n\n"
            f"- Limit notifications to 5 per day per pair:\n"
            f"🔔 Alert Limit: 5"
        )
        await message.reply(first_message, parse_mode='HTML')

        # Define the reply keyboard with Pump and Dump options only
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🟢 Pump Period"), KeyboardButton(text="➗ Pump Percentage")],
                [KeyboardButton(text="🔴 Dump Period"), KeyboardButton(text="➗ Dump Percentage")],
                [KeyboardButton(text="🔔 Alert Limit")],
                [KeyboardButton(text="Back")]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        # Second message: Display current settings
        second_message = (
            f"<b>Your current notification settings are:</b>\n\n"
            f"🟢 Pump Period: <b>{p_index}</b> min\n"
            f"➗ Pump Percentage: <b>{p_percent}%</b>\n\n"
            f"🔴 Dump Period: <b>{d_index}</b> min\n"
            f"➗ Dump Percentage: <b>{d_percent}%</b>\n\n"
            f"🔔 Alert Limit: <b>{'Not set' if alert_limit == 100 else ('Unlimited' if alert_limit is None else f'{alert_limit} per day')}</b>"
        )
        await message.reply(second_message, reply_markup=keyboard, parse_mode='HTML')

        if active == 0 and test == 0:
            trial_message = (
                "⚠️ <b>Your trial period has ended.</b> ⚠️\n"
                "Please renew your subscription via <b>Payment Settings -> Make a payment</b>."
            )
            await message.reply(trial_message, parse_mode='HTML')

        db.close()

    except sqlite3.Error as e:
        error_message = f"Database error in price_show_bot_settings: {e}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
    except Exception as e:
        error_message = f"Unexpected error in price_show_bot_settings: {e}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


@price_router.message(F.text == "Payment Settings")
async def price_show_payment_settings(message: Message):
    try:
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Make a payment"), KeyboardButton(text="Check profile")],
                [KeyboardButton(text="Back")]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        
        await message.reply(
            "💲To unlock full access to the bot's features, please make a payment by selecting the <b>'Make a Payment'</b> option below. By doing so, you will unlock the full functionality of the <b>Pump Bot</b>.\n\n"
            "👤 Choose <b>'Check Profile'</b> to review the current status of your account.",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    except Exception as e:
        error_message = f"Error in price_show_payment_settings: {e}"
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


@price_router.message(F.text == "Make a payment")
async def price_make_payment(message: Message):
    chat_id = message.chat.id
    current_time = datetime.now().strftime("%H:%M:%S")

    # Logging the initiation of the payment
    logger.info(f"{current_time} - Payment initiated - Chat ID: {chat_id}")
    debug_message = f"{current_time} - Payment initiated - Chat ID: {chat_id}"
    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)

    # First message with payment instructions
    first_message = (
        "⭐️ By making a payment, you will gain access to  the <b>Pump Bot</b>. "
        "We offer the following plans:\n\n"
        "1️⃣<b>$Price1 – 1 month of access</b>\n"
        "2️⃣<b>$Price2   – 2 months of access</b>\n"
        "3️⃣<b>$Price3  – 3 months of access</b>\n\n"
        "You can pay using the following options: Tether (TRC20, ERC20), Bitcoin, Ethereum, and Litecoin.\n\n"
        "❓<b>How to Pay:</b>\n\n"
        "1. Once you choose payment plan and press coresponding button, you will be redirected to an invoice page where you can choose your preferred payment method and network. Once selected, click <b>\"Pay\"</b>.\n\n"
        "2. After that, you will see the wallet details and a QR code to complete the payment. The page will also display the exact amount to send, calculated according to the current exchange rate. "
        "3. Once the funds are transferred and confirmed, your access to the bot will be granted automatically within 1-3 minutes.\n\n"
        "❗️<b>Important Information:</b>\n\n"
        "- Payments are processed automatically. You must transfer the full amount in one transaction. Multiple smaller deposits will not be processed, and a refund is <b><i>not guaranteed</i></b>.\n\n"
        "- Once an invoice is generated, you have 24 hours to complete the payment. The invoice expiry date is also shown on the invoice page. "
        "If you miss this window and invoice expired, you will need to generate a new invoice by pressing the <b>Pay</b> button again. "
        "Depositing to an expired invoice may result in the <b>permanent loss of funds</b>.\n\n"
        "- If you encounter any issues or have questions, feel free to contact us at <a href='https://t.me/opportunity_trading'>t.me/opportunity_trading</a>."
    )
    await message.reply(first_message, parse_mode='HTML')

    # Creating inline keyboard buttons with different payment amounts and passing the Telegram chat ID
    base_url_1 = f"http://opportunity-trading.online/...?{chat_id}"
    base_url_2 = f"http://opportunity-trading.online/...?{chat_id}"
    base_url_3 = f"http://opportunity-trading.online/...?{chat_id}"
    
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="Price = 1 month", url=base_url_1))
    builder.add(InlineKeyboardButton(text="Price = 2 months", url=base_url_2))
    builder.add(InlineKeyboardButton(text="Price = 3 months", url=base_url_3))
    builder.adjust(1)  # One button per row
    
    # Second message with the updated inline buttons
    await message.reply("Click the button below to generate a payment invoice:", reply_markup=builder.as_markup())


@price_router.message(F.text == "Check profile")
async def price_check_profile(message: Message):
    chat_id = message.chat.id
    user_name = message.from_user.first_name  # Fetches the user's first name

    try:
        # Connect to the SQLite database
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()

        # Fetch Active, EndDate, and Referral from the whitelist
        cursor.execute('SELECT Active, EndDate, Referral FROM whitelist WHERE TelegramID = ?', (chat_id,))
        result = cursor.fetchone()

        if result:
            active, end_date, referral = result
            referral_display = referral if referral else "None"

            if active == 1:
                # User has active access
                profile_message = (
                    f"👨‍💻User name: {user_name}\n"
                    f"🆔User ID: {chat_id}\n"
                    f"🤝Referral Code: {referral_display}\n"
                    f"🕔Access until: {end_date}"
                )
            else:
                # User's access is inactive
                profile_message = (
                    f"👨‍💻User name: {user_name}\n"
                    f"🆔User ID: {chat_id}\n"
                    f"🤝Referral Code: {referral_display}\n"
                    "⚠️<b>Access denied. Please make a payment in order to access Bot's functionality</b>⚠️"
                )
        else:
            # If no user data found in whitelist
            profile_message = "No profile information found."

        # Send the profile message to the user
        await message.reply(profile_message, parse_mode='HTML')

        # Close the database connection
        db.close()

    except sqlite3.Error as e:
        error_message = f"Database error in price_check_profile: {e}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


@price_router.message(F.text == "Back")
async def price_handle_back(message: Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Bot Settings"), KeyboardButton(text="Payment Settings")],
            [KeyboardButton(text="Contact Support")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.reply("Returning to the Main Menu", reply_markup=keyboard)


@price_router.message(F.text == "Cancel")
async def price_handle_cancel(message: Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🟢 Pump Period"), KeyboardButton(text="➗ Pump Percentage")],
            [KeyboardButton(text="🔴 Dump Period"), KeyboardButton(text="➗ Dump Percentage")],
            [KeyboardButton(text="🔔 Alert Limit")],
            [KeyboardButton(text="Back")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.reply("Input Cancelled", reply_markup=keyboard)


@price_router.message(F.text == "Contact Support")
async def price_support(message: Message):
    # Define the chat URL
    support_url = 'https://t.me/opportunity_trading'
    
    # Send the message with a link to the support chat
    await message.reply(
        "💬 <b>Do you have any questions?</b>\n"
        "Then please do not hesitate to ask! You can contact us through "
        f"<a href='{support_url}'>this chat</a>.\n\n"
        "<b>We will reply as soon as possible. </b>",
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@debug_router.message(Command("send_message"))
async def price_handle_send_message(message: Message):
    chat_id = message.chat.id

    message_to_send = message.text.replace("/send_message", "", 1).strip()
    if not message_to_send:
        await message.reply("Don't forget a text. Format: /send_message *text*")
        return

    # Send the message to all users
    for user_chat_id in bot_data.keys():
        try:
            await price_bot.send_message(chat_id=user_chat_id, text=message_to_send)
        except Exception as e:
            error_message = f"⚠️ Failed to send message to {user_chat_id}: {e} ⚠️"
            print(error_message)
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)

    await message.reply("Message sent.")


#=========================================================================================================================================================================
#=================================================================================SETTING PREFERENCES=====================================================================
#=========================================================================================================================================================================

# Function to handle awaiting preferences
@price_router.message(F.text == "🟢 Pump Period")
@telegram_error_handler
async def awaiting_pump_index(message: Message):
    chat_id = message.chat.id
    
    # Store that we're awaiting this type of input from this user
    user_data[chat_id] = {'awaiting': 'pump_index'}
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    current_value = bot_data.get(chat_id, {}).get('pump_index', 'Not set')
    await message.reply(
        f"Your current 🟢 <b>Pump Period</b> is <b>{current_value}</b>\n"
        "Please, set your new 🟢 <b>Pump Period</b> (1-30 minutes):",
        parse_mode='HTML',
        reply_markup=keyboard
    )


@price_router.message(F.text == "➗ Pump Percentage")
@telegram_error_handler
async def awaiting_pump_threshold(message: Message):
    chat_id = message.chat.id
    
    # Store that we're awaiting this type of input from this user
    user_data[chat_id] = {'awaiting': 'pump_threshold'}
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    current_value = bot_data.get(chat_id, {}).get('pump_threshold', 'Not set')
    await message.reply(
        f"Your current ➗ <b>Pump Percentage</b> is <b>{current_value}%</b>\n"
        "Please, set your new ➗ <b>Pump Percentage</b> (1-100%):",
        parse_mode='HTML',
        reply_markup=keyboard
    )


@price_router.message(F.text == "🔴 Dump Period")
@telegram_error_handler
async def awaiting_dump_index(message: Message):
    chat_id = message.chat.id
    
    # Store that we're awaiting this type of input from this user
    user_data[chat_id] = {'awaiting': 'dump_index'}
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    current_value = bot_data.get(chat_id, {}).get('dump_index', 'Not set')
    await message.reply(
        f"Your current 🔴 <b>Dump Period</b> is <b>{current_value}</b>\n"
        "Please, set your new 🔴 <b>Dump Period</b> (1-30 minutes):",
        parse_mode='HTML',
        reply_markup=keyboard
    )


@price_router.message(F.text == "➗ Dump Percentage")
@telegram_error_handler
async def awaiting_dump_threshold(message: Message):
    chat_id = message.chat.id
    
    # Store that we're awaiting this type of input from this user
    user_data[chat_id] = {'awaiting': 'dump_threshold'}
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    current_value = bot_data.get(chat_id, {}).get('dump_threshold', 'Not set')
    await message.reply(
        f"Your current ➗ <b>Dump Percentage</b> is <b>{current_value}%</b>\n"
        "Please, set your new ➗ <b>Dump Percentage</b> (1-100%):",
        parse_mode='HTML',
        reply_markup=keyboard
    )


@price_router.message(F.text == "🔔 Alert Limit")
@telegram_error_handler
async def awaiting_alert_limit(message: Message):
    chat_id = message.chat.id
    
    # Store that we're awaiting this type of input from this user
    user_data[chat_id] = {'awaiting': 'alert_limit'}
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    current_value = bot_data.get(chat_id, {}).get('alert_limit', 100)
    display_value = 'Not set' if current_value == 100 else current_value
    
    await message.reply(
        f"Your current 🔔 is {display_value}\n"
        "Please set your new 🔔 Alert Limit (1-20 notifications per pair per day).\n"
        "<i>In case you want to receive all the alerts, please write 'all'. </i>",
        parse_mode='HTML',
        reply_markup=keyboard
    )


# Handle text input for all settings
@price_router.message(lambda message: message.text and not message.text.startswith('/') and not message.text in [
    "Bot Settings", "Payment Settings", "Contact Support", "Make a payment", "Check profile", "Back", "Cancel",
    "🟢 Pump Period", "➗ Pump Percentage", "🔴 Dump Period", "➗ Dump Percentage", "🔔 Alert Limit"
])
@telegram_error_handler
async def price_set_pref(message: Message):
    query = message.text
    chat_id = message.chat.id

    # If no awaiting setting type
    if chat_id not in user_data or 'awaiting' not in user_data[chat_id]:
        await message.reply(
            "Please use the buttons below ⤵️\n"
            "<b><i>If you do not see any buttons, try writing /start</i> </b>",
            parse_mode='HTML'
        )
        return

    setting_type_key = user_data[chat_id].get('awaiting')
    setting_type_map = {
        'pump_index': ('pump_index', int, 1, 30, "Please choose a number from 1 to 30"),
        'pump_threshold': ('pump_threshold', float, 1, 100, "Please choose a number from 1 to 100"),
        'dump_index': ('dump_index', int, 1, 30, "Please choose a number from 1 to 30"),
        'dump_threshold': ('dump_threshold', float, 1, 100, "Please choose a number from 1 to 100"),
        'alert_limit': ('alert_limit', int, 1, 20, "Please choose a number from 1 to 20, or type 'all' to receive all notifications")
    }

    setting_info = setting_type_map.get(setting_type_key)
    if not setting_info:
        await message.reply("Invalid setting type.")
        return

    setting_name, value_processor, min_val, max_val, error_msg = setting_info

    # Handle 'all' input for alert_limit
    if setting_name == 'alert_limit' and query.lower() == 'all':
        value = None  # None represents unlimited alerts
    else:
        try:
            value = value_processor(query)
            if not (min_val <= value <= max_val):
                raise ValueError("Value out of range")
        except ValueError:
            await message.reply(error_msg)
            current_time = datetime.now().strftime("%H:%M:%S")
            debug_message = f"❗️{current_time} {chat_id} failed to change preferences: {setting_name}. User sent: {query}❗️"
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)
            return

    # Update the setting in bot_data
    if chat_id in bot_data:
        bot_data[chat_id][setting_name] = value
    else:
        # If not in bot_data, initialize with default values
        bot_data[chat_id] = {
            'pump_index': 2,
            'pump_threshold': 10,
            'dump_index': 2,
            'dump_threshold': 8,
            'alert_limit': 20
        }
        bot_data[chat_id][setting_name] = value

    # Map the setting to the correct database column
    db_column_map = {
        'pump_index': 'Pindex',       # Pump index
        'pump_threshold': 'Ppercent', # Pump percent
        'dump_index': 'Dindex',
        'dump_threshold': 'Dpercent',
        'alert_limit': 'Filter'
    }

    db_column = db_column_map[setting_name]

    # Update the user's setting in the database
    db = sqlite3.connect(WHITELIST_DB_PATH)
    cursor = db.cursor()
    cursor.execute(f'UPDATE whitelist SET {db_column} = ? WHERE TelegramID = ?', (value, chat_id))
    db.commit()
    db.close()

    # Clear the awaiting flag
    if chat_id in user_data:
        del user_data[chat_id]['awaiting']

    # Define the reply keyboard for settings (Pump & Dump only)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🟢 Pump Period"), KeyboardButton(text="➗ Pump Percentage")],
            [KeyboardButton(text="🔴 Dump Period"), KeyboardButton(text="➗ Dump Percentage")],
            [KeyboardButton(text="🔔 Alert Limit")],
            [KeyboardButton(text="Back")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    # Prepare display names
    setting_display_name = {
        'pump_index': '🟢 Pump Period',
        'pump_threshold': '➗ Pump Percentage',
        'dump_index': '🔴 Dump Period',
        'dump_threshold': '➗ Dump Percentage',
        'alert_limit': '🔔 Alert Limit'
    }

    # Prepare the value to display
    display_value = 'Unlimited' if (setting_name == 'alert_limit' and value is None) else f"{value}"

    await message.reply(
        f"<b>{setting_display_name[setting_name]} is set to {display_value}</b>",
        parse_mode='HTML',
        reply_markup=keyboard
    )


#===================================================================================================================================================
#============================================================STARTING===============================================================================
#===================================================================================================================================================

async def main():
    global user_message_counts, message_queue, total_messages_queued, total_messages_sent, blocked_users
    global last_message_time, user_flood_timeout
    global fetch_errors, last_error_message_time
    
    # Initialize counters and tracking variables
    user_message_counts = {}
    message_queue = []
    total_messages_queued = 0
    total_messages_sent = 0
    blocked_users = set()
    last_message_time = {}
    user_flood_timeout = {}
    fetch_errors = []
    last_error_message_time = 0

    # Register all handlers
    price_dp.include_router(price_router)
    debug_dp.include_router(debug_router)

    # Start the bots
    await price_bot.delete_webhook(drop_pending_updates=True)
    await debug_bot.delete_webhook(drop_pending_updates=True)

    # Load user data
    load_user_data()

    # Start polling in separate tasks
    price_polling_task = asyncio.create_task(price_dp.start_polling(price_bot))
    debug_polling_task = asyncio.create_task(debug_dp.start_polling(debug_bot))

    # Calculate time until the next minute
    now = datetime.now()
    delay = 60 - now.second - now.microsecond / 1000000.0
    await asyncio.sleep(delay)  # Wait until the start of the next minute

    # Initial reinitialization at startup
    await reinitialize_pairs()

    try:
        # Main loop for fetching and comparing prices
        while True:
            try:
                current_time = datetime.now()

                # Clear the fetch_errors list at the start of each loop
                fetch_errors = []

                # Fetch and compare prices for the tracked pairs — now in bulk
                start_time = current_time.strftime("%H:%M:%S")
                price_fetched_count = await price_fetch_and_compare_prices()

                # Send a summary error message if there were errors and if the interval has passed
                current_time_sec = time.time()
                if fetch_errors and (current_time_sec - last_error_message_time > ERROR_MESSAGE_INTERVAL):
                    error_message = "The following errors occurred during price fetching:\n" + "\n".join(fetch_errors)
                    print(error_message)
                    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                    last_error_message_time = current_time_sec  # Update the timestamp

                # Check for notifications to send based on new price data
                await price_check_and_send_notifications()

                # Process the message queue
                await process_message_queue()

                end_time = datetime.now().strftime("%H:%M:%S")

                # **Get Active and Test Users Counts**
                try:
                    db = sqlite3.connect(WHITELIST_DB_PATH)
                    cursor = db.cursor()

                    # Get the count of active users
                    cursor.execute('SELECT COUNT(*) FROM whitelist WHERE Active = 1')
                    active_users_count = cursor.fetchone()[0]

                    # Get the count of test users
                    cursor.execute('SELECT COUNT(*) FROM whitelist WHERE Test = 1')
                    test_users_count = cursor.fetchone()[0]

                    # Close the database connection
                    db.close()
                except sqlite3.Error as e:
                    error_message = f"Database error when fetching user counts: {e}"
                    print(error_message)
                    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                    # Set counts to zero in case of error
                    active_users_count = 0
                    test_users_count = 0

                # Prepare and send the debug message
                debug_message = (
                    f"{start_time} -> {end_time} - Data collected\n"
                    f"Prices: {price_fetched_count} Fetched.\n"
                    f"Queued: {total_messages_queued} | Sent: {total_messages_sent}\n"
                    f"Active Users: {active_users_count} | Test Users: {test_users_count}"
                )
                print(debug_message)
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)

                # Reinitialization: Run it after regular processing at a specific minute
                if current_time.minute % 60 == 0:  # Reinitialize every 60 minutes
                    await reinitialize_pairs()

                # Reset per-user message counts and message queue for the next minute
                user_message_counts = {}
                message_queue = []
                total_messages_queued = 0
                total_messages_sent = 0
                blocked_users = set()
                last_message_time = {}
                user_flood_timeout = {}

                # Wait until the start of the next minute
                now = datetime.now()
                delay = 60 - now.second - now.microsecond / 1000000.0
                await asyncio.sleep(delay)

            except Exception as e:
                # Log the error and retry after a small delay
                error_message = f"An unexpected error occurred in the main loop: {e}"
                logger.error(error_message, exc_info=True)
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                await asyncio.sleep(2)  # Small delay before retrying

    except KeyboardInterrupt:
        # Gracefully handle keyboard interruption
        print("Bot interrupted by user, shutting down gracefully...")
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text="Bot interrupted by user, shutting down gracefully...")
    
    finally:
        # Cancel polling tasks
        price_polling_task.cancel()
        debug_polling_task.cancel()
        
        # Close exchange
        await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())