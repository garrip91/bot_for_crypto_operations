import asyncio
import signal
from contextlib import suppress
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

from dotenv import load_dotenv
import os


load_dotenv()

#==========================================================================================================================
#======================================================= INITIALIZATION ====================================================
#==========================================================================================================================

#----------------------------------------------- BINANCE SETUP -------------------------------------------------------------

# Binance setup with async support
exchange_binance = ccxt.binance({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'  # Switch to USDT-M Futures markets
    }
})

#----------------------------------------------- BINANCE SETUP -------------------------------------------------------------

# Binance setup with async support
exchange_bybit = ccxt.bybit({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'  # USDT-M Futures markets
    }
})

#-------------------------------------------- PRICE BOT INITIALIZATION -----------------------------------------------------

prices = {'binance': {}, 'bybit': {}}  # Prices by exchange
open_interest = {'binance': {}, 'bybit': {}}  # Open Interest by exchange
prices_cooldown = {'binance': {}, 'bybit': {}}  # Cooldown by exchange
oi_cooldown = {'binance': {}, 'bybit': {}}  # Separate Cooldown for OI by exchange
all_pairs = {'binance': [], 'bybit': []}  # Store all available pairs by exchange
ignored_pairs = set()  # Store ignored pairs from the ban table (shared for now)

PRICE_TELEGRAM_TOKEN = os.getenv("PRICE_TELEGRAM_TOKEN")
DEBUG_BOT_TOKEN = os.getenv("DEBUG_BOT_TOKEN")
DEBUG_CHAT_ID = os.getenv("DEBUG_CHAT_ID")

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

#WHITELIST_DB_PATH = "/var/www/site/payment/whitelist.db"
WHITELIST_DB_PATH = "../databases_modified_whitelist/whitelist.db"

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
    ignored_pairs = get_ignored_pairs()  # Shared ignored pairs

    exchanges = {
        'binance': exchange_binance,
        'bybit': exchange_bybit
    }
    fetched_summary = {}

    for exchange_name, exchange in exchanges.items():
        print(f"**** [[ Starting fetch for {exchange_name}... ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Начало обработки
        # Load all available USDT-M PERPETUAL markets
        markets = await exchange.load_markets()

        # Отладка структуры markets для Bybit
        if exchange_name == 'bybit' and markets:
            sample_pair, sample_data = list(markets.items())[0]
            print(f"****[[ Bybit sample market: {sample_pair}: {sample_data} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА

        # ВРЕМЕННАЯ ОТЛАДКА СТРУКТУРЫ "markets":
        if exchange_name == 'bybit':
            all_pairs[exchange_name] = [
                pair for pair, data in markets.items()
                if data.get('quote') == 'USDT' and data.get('type') == 'swap' and data.get('linear', False)
            ]
        else:  # для Binance
            all_pairs[exchange_name] = [
                pair for pair, data in markets.items()
                if data.get('info', {}).get('contractType') == 'PERPETUAL' and data.get('quote') == 'USDT'
            ]
        print(f"****[[ {exchange_name} total pairs from load_markets: {len(all_pairs[exchange_name])} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Сколько пар найдено
        print(f"****[[ {exchange_name} first 5 pairs: {all_pairs[exchange_name][:5]} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Первые 5 пар для проверки формата

        pairs = [pair for pair in all_pairs[exchange_name] if pair not in ignored_pairs]
        print(f"****[[ {exchange_name} pairs after filtering ignored: {len(pairs)} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: После фильтрации игнорируемых пар

        # Fetch tickers and OI
        try:
            all_tickers = await exchange.fetch_tickers(pairs)
            print(f"****[[ {exchange_name} tickers fetched: {len(all_tickers)} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Сколько тикеров получено
            print(f"****[[ {exchange_name} sample ticker data: {list(all_tickers.items())[:2]} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Пример данных тикеров
        except Exception as e:
            print(f"****[[ Error fetching tickers for {exchange_name}: {e} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА
            all_tickers = {}
        
        all_oi = {}
        if exchange_name == 'bybit':
            try:
                for pair in pairs:
                    try:
                        oi_data = await exchange.fetch_open_interest(pair)
                        all_oi[pair] = oi_data
                        print(f"****[[ {exchange_name} OI fetched for {pair}: {oi_data} ]]****")
                    except Exception as e:
                        print(f"****[[ Failed to fetch OI for {pair} on {exchange_name}: {e} ]]****")
                print(f"****[[ {exchange_name} OI fetched: {len(all_oi)} ]]****")
                # Добавляем отладку первых 5 пар OI:
                if all_oi:
                    print(f"****[[ {exchange_name} first 5 OI pairs: {list(all_oi.items())[:5]} ]]****")
            except Exception as e:
                print(f"****[[ Error fetching OI for {exchange_name}: {e} ]]****")

        fetched, skipped = 0, 0
        for pair in pairs:
            ticker = all_tickers.get(pair)
            oi = all_oi.get(pair, {}).get('openInterest') if exchange_name == 'bybit' else None

            if ticker and ticker.get('last') is not None:
                prices[exchange_name][pair] = [ticker['last']]
                fetched += 1
            else:
                skipped += 1

            if oi is not None:
                open_interest[exchange_name][pair] = [oi]

        fetched_summary[exchange_name] = (fetched, skipped, len([p for p in all_pairs[exchange_name] if p in ignored_pairs]))
        print(f"****[[ {exchange_name} summary: {fetched} fetched, {skipped} skipped, {fetched_summary[exchange_name][2]} ignored ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Итог

    end_time = datetime.now().strftime("%H:%M:%S")
    summary_message = (
        f"{start_time} -> {end_time} - Initial prices collected\n"
        f"Binance: {fetched_summary['binance'][0]} Fetched, {fetched_summary['binance'][1]} Skipped, {fetched_summary['binance'][2]} Ignored\n"
        f"Bybit: {fetched_summary['bybit'][0]} Fetched, {fetched_summary['bybit'][1]} Skipped, {fetched_summary['bybit'][2]} Ignored"
    )
    print(summary_message)
    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)

    # Return successfully fetched pairs for each exchange
    return {ex: [p for p in all_pairs[ex] if p in prices[ex]] for ex in exchanges}


@global_timeout_retry(retries=3, delay=5)
async def price_fetch_and_compare_prices():
    try:
        exchanges = {'binance': exchange_binance, 'bybit': exchange_bybit}
        fetched_count = {'binance': 0, 'bybit': 0}

        for exchange_name, exchange in exchanges.items():
            tracked_pairs = [pair for pair in all_pairs[exchange_name] if pair not in ignored_pairs]
            print(f"***[[ {exchange_name} tracked pairs: {len(tracked_pairs)} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Сколько пар отслеживается
            
            if not tracked_pairs:
                print(f"****[[ No tracked pairs for {exchange_name}, skipping... ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА
                continue
            
            try:
                all_tickers = await exchange.fetch_tickers(tracked_pairs)
                print(f"****[[ {exchange_name} tickers fetched: {len(all_tickers)} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Сколько тикеров обновлено
            except Exception as e:
                print(f"****[[ Error fetching tickers for {exchange_name}: {e} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА
                all_tickers = {}
            
            all_oi = {}
            if exchange_name == 'bybit':
                try:
                    for pair in tracked_pairs:
                        oi_data = await exchange.fetch_open_interest(pair)
                        all_oi[pair] = oi_data.get('openInterestAmount', 0)
                    print(f"****[[ {exchange_name} OI fetched: {len(all_oi)} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Сколько OI обновлено
                except Exception as e:
                    print(f"****[[ Error fetching OI for {exchange_name}: {e} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА

            async with prices_lock:
                for pair in tracked_pairs:
                    ticker_data = all_tickers.get(pair, {})
                    new_price = ticker_data.get('last')
                    new_oi = all_oi.get(pair, {}).get('openInterest') if exchange_name == 'bybit' else None

                    if new_price is not None:
                        if pair in prices[exchange_name]:
                            prices[exchange_name][pair].insert(0, new_price)
                            if len(prices[exchange_name][pair]) > 30:
                                prices[exchange_name][pair].pop()
                        else:
                            prices[exchange_name][pair] = [new_price]
                        fetched_count[exchange_name] += 1

                    if new_oi is not None:
                        if pair in open_interest[exchange_name]:
                            open_interest[exchange_name][pair].insert(0, new_oi)
                            if len(open_interest[exchange_name][pair]) > 30:
                                open_interest[exchange_name][pair].pop()
                        else:
                            open_interest[exchange_name][pair] = [new_oi]

        print(f"****[[ Fetched counts: {fetched_count} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Итоговое количество обновлённых пар
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
        ignored_pairs = get_ignored_pairs()
        exchanges = {'binance': exchange_binance, 'bybit': exchange_bybit}
        fetched_summary = {}

        for exchange_name, exchange in exchanges.items():
            print(f"****[[ Reinitializing {exchange_name}... ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Начало переинициализации
            try:
                markets = await exchange.load_markets()
                
                # ДОБАВЛЯЕМ ОТЛАДКУ ДЛЯ Bybit:
                if exchange_name == 'bybit' and markets:
                    sample_pair, sample_data = list(markets.items())[0] # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Первая пара для примера
                    print(f"****[[ Bybit sample market: {sample_pair}: {sample_data} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА
                
                if exchange_name == 'bybit':
                    current_all_pairs = [
                        pair for pair, data in markets.items()
                        if data.get('quote') == 'USDT' and data.get('type') == 'swap' and data.get('linear', False)
                    ]
                else:  # Binance
                    current_all_pairs = [
                        pair for pair, data in markets.items()
                        if data.get('info', {}).get('contractType') == 'PERPETUAL' and data.get('quote') == 'USDT'
                    ]
                print(f"****[[ {exchange_name} total pairs from load_markets: {len(current_all_pairs)} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Сколько пар найдено
                print(f"****[[ {exchange_name} first 5 pairs: {current_all_pairs[:5]} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА

                async with prices_lock:
                    for pair in list(prices[exchange_name].keys()):
                        if pair in ignored_pairs or not prices[exchange_name][pair]:
                            del prices[exchange_name][pair]

                try:
                    all_tickers = await exchange.fetch_tickers(current_all_pairs)
                    print(f"****[[ {exchange_name} tickers fetched: {len(all_tickers)} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Сколько тикеров получено
                except Exception as e:
                    print(f"****[[ Error fetching tickers for {exchange_name}: {e} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА
                    all_tickers = {}

                all_oi = {}
                if exchange_name == 'bybit':
                    for pair in current_all_pairs:
                        try:
                            oi_data = await exchange.fetch_open_interest(pair)
                            all_oi[pair] = oi_data
                            print(f"****[[ {exchange_name} OI fetched for {pair}: {oi_data} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: OI для каждой пары
                        except Exception as e:
                            print(f"****[[ Failed to fetch OI for {pair} on {exchange_name}: {e} ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА
                    # Добавляем отладку первых 5 пар OI:
                    if all_oi:
                        print(f"****[[ {exchange_name} first 5 OI pairs: {list(all_oi.items())[:5]} ]]****")

                fetched, skipped, ignored_count = 0, 0, 0
                for pair in current_all_pairs:
                    if pair in ignored_pairs:
                        ignored_count += 1
                        continue

                    ticker = all_tickers.get(pair, {})
                    latest_price = ticker.get('last')
                    latest_oi = all_oi.get(pair, {}).get('openInterest') if exchange_name == 'bybit' else None

                    if latest_price is not None:
                        async with prices_lock:
                            if pair in prices[exchange_name]:
                                prices[exchange_name][pair].insert(0, latest_price)
                                if len(prices[exchange_name][pair]) > 30:
                                    prices[exchange_name][pair].pop()
                            else:
                                prices[exchange_name][pair] = [latest_price]
                        fetched += 1
                    else:
                        skipped += 1

                    if latest_oi is not None:
                        async with prices_lock:
                            if pair in open_interest[exchange_name]:
                                open_interest[exchange_name][pair].insert(0, latest_oi)
                                if len(open_interest[exchange_name][pair]) > 30:
                                    open_interest[exchange_name][pair].pop()
                            else:
                                open_interest[exchange_name][pair] = [latest_oi]

                all_pairs[exchange_name] = current_all_pairs
                fetched_summary[exchange_name] = (fetched, skipped, ignored_count)
                print(f"****[[ {exchange_name} summary: {fetched} fetched, {skipped} skipped, {ignored_count} ignored ]]****") # ДЛЯ ОТЛАДОЧНОГО ВЫВОДА: Итог
            except Exception as e:
                error_message = f"Error reinitializing {exchange_name}: {e}\n{traceback.format_exc()}"
                print(error_message)
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                fetched_summary[exchange_name] = (0, 0, 0)

        async with prices_lock:
            for exchange in ['binance', 'bybit']:
                for pair in list(prices_cooldown[exchange].keys()):
                    if pair not in prices[exchange]:
                        del prices_cooldown[exchange][pair]
                for pair in list(oi_cooldown[exchange].keys()):
                    if pair not in open_interest[exchange]:
                        del oi_cooldown[exchange][pair]

        end_time = datetime.now().strftime("%H:%M:%S")
        summary_message = (
            f"{start_time} -> {end_time} - Re-initialization done\n"
            f"Binance: {fetched_summary['binance'][0]} Fetched, {fetched_summary['binance'][1]} Skipped, {fetched_summary['binance'][2]} Ignored\n"
            f"Bybit: {fetched_summary['bybit'][0]} Fetched, {fetched_summary['bybit'][1]} Skipped, {fetched_summary['bybit'][2]} Ignored"
        )
        print(summary_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)

    except Exception as e:
        error_message = f"Critical reinitialization error: {e}\n{traceback.format_exc()}"
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
    cursor.execute('SELECT 1 FROM whitelist WHERE TelegramID = ? AND Active = 1 AND Blocked = 0', (chat_id,))
    result = cursor.fetchone()

    # Close the database connection
    db.close()

    # Return True if the user is found and active, otherwise False
    return result is not None


@global_timeout_retry(retries=3, delay=5)
async def price_send_alert(exchange, pair, change_percent, old_value, new_value, value_list, condition_type, settings, chat_id):
    global message_queue, total_messages_queued, notification_counters

    # Assign emojis and signal names based on condition type
    if condition_type == 'Short':
        emoji = '🟢'
        signal_name = 'Pump Signal'
        period = f"{settings['pump_index']} min"
    elif condition_type == 'Dump':
        emoji = '🔴'
        signal_name = 'Dump Signal'
        period = f"{settings['dump_index']} min"
    elif condition_type == 'OI_Increase':
        emoji = '📈'
        signal_name = 'OI Increase'
        period = f"{settings['oi_index']} min"
    elif condition_type == 'OI_Decrease':
        emoji = '📉'
        signal_name = 'OI Decrease'
        period = f"{settings['oi_index']} min"
    else:
        emoji = 'ℹ️'
        signal_name = condition_type
        period = ''
    
    exchange_emoji = '🔶' if exchange == 'binance' else '🔷'
    exchange_name = 'Binance' if exchange == 'binance' else 'Bybit'

    raw_symbol = pair.replace(':USDT', '').replace('/', '')
    pair_display = raw_symbol.replace('USDT', '')
    url = f"https://www.tradingview.com/chart/?symbol={exchange_name.upper()}%3A{raw_symbol}.P"
    hyperlink = f'<a href="{url}">{pair_display}</a>'

    # Format values
    formatted_old = f"{old_value:.8f}".rstrip('0').rstrip('.') if isinstance(old_value, float) else str(old_value)
    formatted_new = f"{new_value:.8f}".rstrip('0').rstrip('.') if isinstance(new_value, float) else str(new_value)

    # Alert numbering
    alert_number = notification_counters[chat_id][pair] + 1

    # Compose the alert message
    message = (
        f"{emoji} <b>{hyperlink}</b> | {signal_name}\n"
        f"{exchange_emoji} {exchange_name} | <i>{period}</i>\n"
        f"{'Price' if 'OI' not in condition_type else 'OI'} Change: <b>{abs(change_percent):.2f}%</b>\n"
        f"{formatted_old} -> <b>{formatted_new}</b>\n"
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
        notification_counters = defaultdict(lambda: defaultdict(int))
        last_counter_reset_date = current_date

    async with prices_lock:
        for exchange in ['binance', 'bybit']:
            for pair, price_list in prices[exchange].items():
                if pair not in prices_cooldown[exchange]:
                    prices_cooldown[exchange][pair] = {}
                if pair not in oi_cooldown[exchange]:
                    oi_cooldown[exchange][pair] = {}

                if len(price_list) == 0:
                    continue  # Skip if no price data

                new_price = price_list[0]
                oi_list = open_interest[exchange].get(pair, [])
                new_oi = oi_list[0] if oi_list else None

                for chat_id, settings in bot_data.items():
                    if not is_user_whitelisted_and_active(chat_id) or settings.get(exchange, 1) == 0:
                        continue

                    alert_limit = settings.get('alert_limit', 20)
                    if notification_counters[chat_id][pair] >= alert_limit:
                        continue

                    if chat_id not in prices_cooldown[exchange][pair]:
                        prices_cooldown[exchange][pair][chat_id] = {'Short': 0, 'Dump': 0}
                    if chat_id not in oi_cooldown[exchange][pair]:
                        oi_cooldown[exchange][pair][chat_id] = {'OI_Increase': 0, 'OI_Decrease': 0}

                    for condition in ['Short', 'Dump']:
                        if prices_cooldown[exchange][pair][chat_id][condition] > 0:
                            prices_cooldown[exchange][pair][chat_id][condition] -= 1
                    for condition in ['OI_Increase', 'OI_Decrease']:
                        if oi_cooldown[exchange][pair][chat_id][condition] > 0:
                            oi_cooldown[exchange][pair][chat_id][condition] -= 1

                    pump_index = settings['pump_index']
                    pump_threshold = settings['pump_threshold']
                    if len(price_list) > pump_index and prices_cooldown[exchange][pair][chat_id]['Short'] == 0:
                        old_price = price_list[pump_index]
                        change_percent = (new_price - old_price) / old_price * 100
                        if change_percent >= pump_threshold:
                            await price_send_alert(exchange, pair, change_percent, old_price, new_price, price_list, 'Short', settings, chat_id)
                            prices_cooldown[exchange][pair][chat_id]['Short'] = pump_index
                            notification_counters[chat_id][pair] += 1
                            continue
                    
                    dump_index = settings['dump_index']
                    dump_threshold = settings['dump_threshold']
                    if len(price_list) > dump_index and prices_cooldown[exchange][pair][chat_id]['Dump'] == 0:
                        old_price = price_list[dump_index]
                        change_percent = (new_price - old_price) / old_price * 100
                        if change_percent <= -dump_threshold:
                            await price_send_alert(exchange, pair, change_percent, old_price, new_price, price_list, 'Dump', settings, chat_id)
                            prices_cooldown[exchange][pair][chat_id]['Dump'] = dump_index
                            notification_counters[chat_id][pair] += 1
                    
                    oi_index = settings.get('oi_index', pump_index)  # Shared with pump_index unless specified
                    oi_threshold = settings.get('oi_threshold', 10.0)  # Default OI threshold
                    if new_oi and len(oi_list) > oi_index:
                        old_oi = oi_list[oi_index]
                        oi_change_percent = (new_oi - old_oi) / old_oi * 100
                        if oi_change_percent >= oi_threshold and oi_cooldown[exchange][pair][chat_id]['OI_Increase'] == 0:
                            await price_send_alert(exchange, pair, oi_change_percent, old_oi, new_oi, oi_list, 'OI_Increase', settings, chat_id)
                            oi_cooldown[exchange][pair][chat_id]['OI_Increase'] = oi_index
                            notification_counters[chat_id][pair] += 1
                        elif oi_change_percent <= -oi_threshold and oi_cooldown[exchange][pair][chat_id]['OI_Decrease'] == 0:
                            await price_send_alert(exchange, pair, oi_change_percent, old_oi, new_oi, oi_list, 'OI_Decrease', settings, chat_id)
                            oi_cooldown[exchange][pair][chat_id]['OI_Decrease'] = oi_index
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
        cursor.execute("SELECT TelegramID, Pindex, Ppercent, Dindex, Dpercent, Filter, Binance, Bybit, Blocked FROM whitelist WHERE Active = 1")
        rows = cursor.fetchall()
        for row in rows:
            telegram_id = int(row[0])
            bot_data[telegram_id] = {
                'pump_index': row[1],
                'pump_threshold': row[2],
                'dump_index': row[3],
                'dump_threshold': row[4],
                'alert_limit': row[5] if row[5] is not None else 100,
                'binance': row[6],
                'bybit': row[7],
                'blocked': row[8]
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
            cursor.execute('SELECT Active, StartDate, EndDate, Pindex, Ppercent, Dindex, Dpercent, Binance, Bybit, Blocked FROM whitelist WHERE TelegramID = ?', (chat_id,))
            result = cursor.fetchone()

            if result:
                # User exists in the database
                active, start_date_db, end_date_db, p_index, p_percent, d_index, d_percent, binance, bybit, blocked = result
                is_new_user = False
                start_date = start_date_db
                end_date = end_date_db
            else:
                # User is new, insert into the database
                trial_days = 30 # Устанавливаем 30 дней для всех новых пользователей
                start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                end_date = (datetime.now() + timedelta(days=trial_days)).strftime('%Y-%m-%d %H:%M:%S')  
                
                # Default values for Pump (previously pump) and Dump settings
                p_index, p_percent = 3, 5
                d_index, d_percent = 2, 8
                active = 1
                binance = 1
                bybit = 1
                blocked = 0
                cursor.execute('''
                    INSERT INTO whitelist (TelegramID, Username, Referral, Active, StartDate, EndDate, Pindex, Ppercent, Dindex, Dpercent, Binance, Bybit, Blocked)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (chat_id, username, referral_code, active, start_date, end_date, p_index, p_percent, d_index, d_percent, binance, bybit, blocked))
                db.commit()
                is_new_user = True

            # Store user settings in bot_data
            bot_data[chat_id] = {
                'pump_index': p_index,
                'pump_threshold': p_percent,
                'dump_index': d_index,
                'dump_threshold': d_percent,
                'binance': binance,
                'bybit': bybit,
                'blocked': blocked
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
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()
        cursor.execute('SELECT Active, Pindex, Ppercent, Dindex, Dpercent, Filter, Binance, Bybit FROM whitelist WHERE TelegramID = ?', (chat_id,))
        user_settings = cursor.fetchone()
        if user_settings:
            active, p_index, p_percent, d_index, d_percent, alert_limit, binance, bybit = user_settings
            alert_limit = alert_limit if alert_limit is not None else 100
        else:
            active, p_index, p_percent, d_index, d_percent, alert_limit, binance, bybit = 1, 3, 5, 2, 8, 100, 1, 1
        bot_data[chat_id] = {
            'pump_index': p_index,
            'pump_threshold': p_percent,
            'dump_index': d_index,
            'dump_threshold': d_percent,
            'alert_limit': alert_limit,
            'binance': binance,
            'bybit': bybit,
            'oi_index': p_index,  # Default to pump_index
            'oi_threshold': 10.0,  # Default OI threshold
            'active': active
        }
        first_message = (
            f"<b>How to change settings:</b>\n"
            f"Press the button for the setting you want to change, then send the desired amount.\n\n"
            f"<i>Signal Periods</i>: 1 to 30 (minutes)\n"
            f"<i>Signal Percentages</i>: 1% to 100%\n"
            f"<i>OI Settings</i>: Period (1-30), Threshold (1-100%)\n"
            f"<i>Alert Limit</i>: 1 to 20 or 'all'\n"
            f"<i>Exchange Toggle</i>: 0 (off) or 1 (on)\n"
        )
        await message.reply(first_message, parse_mode='HTML')
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🟢 Pump Period"), KeyboardButton(text="➗ Pump Percentage")],
                [KeyboardButton(text="🔴 Dump Period"), KeyboardButton(text="➗ Dump Percentage")],
                [KeyboardButton(text="📊 OI Period"), KeyboardButton(text="📈 OI Threshold")],
                [KeyboardButton(text="🔶 Binance Toggle"), KeyboardButton(text="🔷 Bybit Toggle")],
                [KeyboardButton(text="🔔 Alert Limit"), KeyboardButton(text="Back")]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        second_message = (
            f"<b>Your current notification settings are:</b>\n\n"
            f"🟢 Pump Period: <b>{p_index}</b> min\n"
            f"➗ Pump Percentage: <b>{p_percent}%</b>\n"
            f"🔴 Dump Period: <b>{d_index}</b> min\n"
            f"➗ Dump Percentage: <b>{d_percent}%</b>\n"
            f"📊 OI Period: <b>{bot_data[chat_id]['oi_index']}</b> min\n"
            f"📈 OI Threshold: <b>{bot_data[chat_id]['oi_threshold']}%</b>\n"
            f"🔶 Binance: <b>{'On' if binance else 'Off'}</b>\n"
            f"🔷 Bybit: <b>{'On' if bybit else 'Off'}</b>\n"
            f"🔔 Alert Limit: <b>{'Unlimited' if alert_limit == 100 else alert_limit}</b>"
        )
        await message.reply(second_message, reply_markup=keyboard, parse_mode='HTML')
        if active == 0:
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


@price_router.message(F.text == "📊 OI Period")
@telegram_error_handler
async def awaiting_oi_index(message: Message):
    chat_id = message.chat.id
    user_data[chat_id] = {'awaiting': 'oi_index'}
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Cancel")]], resize_keyboard=True, one_time_keyboard=True)
    current_value = bot_data.get(chat_id, {}).get('oi_index', 'Not set')
    await message.reply(f"Your current 📊 <b>OI Period</b> is <b>{current_value}</b>\nPlease, set your new OI Period (1-30 minutes):", parse_mode='HTML', reply_markup=keyboard)


@price_router.message(F.text == "📈 OI Threshold")
@telegram_error_handler
async def awaiting_oi_threshold(message: Message):
    chat_id = message.chat.id
    user_data[chat_id] = {'awaiting': 'oi_threshold'}
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Cancel")]], resize_keyboard=True, one_time_keyboard=True)
    current_value = bot_data.get(chat_id, {}).get('oi_threshold', 'Not set')
    await message.reply(f"Your current 📈 <b>OI Threshold</b> is <b>{current_value}%</b>\nPlease, set your new OI Threshold (1-100%):", parse_mode='HTML', reply_markup=keyboard)


@price_router.message(F.text == "🔶 Binance Toggle")
@telegram_error_handler
async def awaiting_binance_toggle(message: Message):
    chat_id = message.chat.id
    user_data[chat_id] = {'awaiting': 'binance'}
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Cancel")]], resize_keyboard=True, one_time_keyboard=True)
    current_value = 'On' if bot_data.get(chat_id, {}).get('binance', 1) else 'Off'
    await message.reply(f"Your current 🔶 <b>Binance</b> setting is <b>{current_value}</b>\nPlease, set 0 (off) or 1 (on):", parse_mode='HTML', reply_markup=keyboard)


@price_router.message(F.text == "🔷 Bybit Toggle")
@telegram_error_handler
async def awaiting_bybit_toggle(message: Message):
    chat_id = message.chat.id
    user_data[chat_id] = {'awaiting': 'bybit'}
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Cancel")]], resize_keyboard=True, one_time_keyboard=True)
    current_value = 'On' if bot_data.get(chat_id, {}).get('bybit', 1) else 'Off'
    await message.reply(f"Your current 🔷 <b>Bybit</b> setting is <b>{current_value}</b>\nPlease, set 0 (off) or 1 (on):", parse_mode='HTML', reply_markup=keyboard)


@price_router.message(lambda message: message.text and not message.text.startswith('/') and not message.text in [
    "Bot Settings", "Payment Settings", "Contact Support", "Make a payment", "Check profile", "Back", "Cancel",
    "🟢 Pump Period", "➗ Pump Percentage", "🔴 Dump Period", "➗ Dump Percentage", "🔔 Alert Limit",
    "📊 OI Period", "📈 OI Threshold", "🔶 Binance Toggle", "🔷 Bybit Toggle"
])
@telegram_error_handler
async def price_set_pref(message: Message):
    query = message.text
    chat_id = message.chat.id

    if chat_id not in user_data or 'awaiting' not in user_data[chat_id]:
        await message.reply("Please use the buttons below ⤵️\n<b><i>If you do not see any buttons, try writing /start</i> </b>", parse_mode='HTML')
        return

    setting_type_key = user_data[chat_id]['awaiting']
    setting_type_map = {
        'pump_index': ('pump_index', int, 1, 30, "Please choose a number from 1 to 30"),
        'pump_threshold': ('pump_threshold', float, 1, 100, "Please choose a number from 1 to 100"),
        'dump_index': ('dump_index', int, 1, 30, "Please choose a number from 1 to 30"),
        'dump_threshold': ('dump_threshold', float, 1, 100, "Please choose a number from 1 to 100"),
        'alert_limit': ('alert_limit', int, 1, 20, "Please choose a number from 1 to 20, or type 'all' to receive all notifications"),
        'oi_index': ('oi_index', int, 1, 30, "Please choose a number from 1 to 30"),
        'oi_threshold': ('oi_threshold', float, 1, 100, "Please choose a number from 1 to 100"),
        'binance': ('binance', int, 0, 1, "Please choose 0 (off) or 1 (on)"),
        'bybit': ('bybit', int, 0, 1, "Please choose 0 (off) or 1 (on)")
    }

    setting_info = setting_type_map.get(setting_type_key)
    if not setting_info:
        await message.reply("Invalid setting type.")
        return

    setting_name, value_processor, min_val, max_val, error_msg = setting_info

    if setting_name == 'alert_limit' and query.lower() == 'all':
        value = None
    else:
        try:
            value = value_processor(query)
            if not (min_val <= value <= max_val):
                raise ValueError("Value out of range")
        except ValueError:
            await message.reply(error_msg)
            return

    if chat_id in bot_data:
        bot_data[chat_id][setting_name] = value
    else:
        bot_data[chat_id] = {
            'pump_index': 2, 'pump_threshold': 10, 'dump_index': 2, 'dump_threshold': 8,
            'alert_limit': 20, 'oi_index': 2, 'oi_threshold': 10.0, 'binance': 1, 'bybit': 1
        }
        bot_data[chat_id][setting_name] = value

    db_column_map = {
        'pump_index': 'Pindex', 'pump_threshold': 'Ppercent', 'dump_index': 'Dindex', 'dump_threshold': 'Dpercent',
        'alert_limit': 'Filter', 'binance': 'Binance', 'bybit': 'Bybit'
    }
    db_column = db_column_map.get(setting_name)
    if db_column:
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()
        cursor.execute(f'UPDATE whitelist SET {db_column} = ? WHERE TelegramID = ?', (value, chat_id))
        db.commit()
        db.close()

    del user_data[chat_id]['awaiting']
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🟢 Pump Period"), KeyboardButton(text="➗ Pump Percentage")],
            [KeyboardButton(text="🔴 Dump Period"), KeyboardButton(text="➗ Dump Percentage")],
            [KeyboardButton(text="📊 OI Period"), KeyboardButton(text="📈 OI Threshold")],
            [KeyboardButton(text="🔶 Binance Toggle"), KeyboardButton(text="🔷 Bybit Toggle")],
            [KeyboardButton(text="🔔 Alert Limit"), KeyboardButton(text="Back")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    setting_display_name = {
        'pump_index': '🟢 Pump Period', 'pump_threshold': '➗ Pump Percentage', 'dump_index': '🔴 Dump Period',
        'dump_threshold': '➗ Dump Percentage', 'alert_limit': '🔔 Alert Limit', 'oi_index': '📊 OI Period',
        'oi_threshold': '📈 OI Threshold', 'binance': '🔶 Binance', 'bybit': '🔷 Bybit'
    }
    display_value = 'Unlimited' if (setting_name == 'alert_limit' and value is None) else ('On' if value == 1 and setting_name in ['binance', 'bybit'] else ('Off' if value == 0 and setting_name in ['binance', 'bybit'] else f"{value}"))
    await message.reply(f"<b>{setting_display_name[setting_name]} is set to {display_value}</b>", parse_mode='HTML', reply_markup=keyboard)


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
    await asyncio.sleep(delay)

    # Initial reinitialization at startup
    await reinitialize_pairs()

    # Создаем событие для остановки
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # Обработка сигналов SIGINT и SIGTERM
    def handle_shutdown():
        print("Received shutdown signal (Ctrl+C), stopping...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    try:
        # Main loop for fetching and comparing prices
        while not stop_event.is_set():
            try:
                current_time = datetime.now()
                fetch_errors = []

                start_time = current_time.strftime("%H:%M:%S")
                price_fetched_count = await price_fetch_and_compare_prices()

                current_time_sec = time.time()
                if fetch_errors and (current_time_sec - last_error_message_time > ERROR_MESSAGE_INTERVAL):
                    error_message = "The following errors occurred during price fetching:\n" + "\n".join(fetch_errors)
                    print(error_message)
                    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                    last_error_message_time = current_time_sec

                await price_check_and_send_notifications()
                await process_message_queue()

                end_time = datetime.now().strftime("%H:%M:%S")
                try:
                    db = sqlite3.connect(WHITELIST_DB_PATH)
                    cursor = db.cursor()
                    cursor.execute('SELECT COUNT(*) FROM whitelist WHERE Active = 1')
                    active_users_count = cursor.fetchone()[0]
                    db.close()
                except sqlite3.Error as e:
                    error_message = f"Database error when fetching user counts: {e}"
                    print(error_message)
                    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                    active_users_count = 0

                debug_message = (
                    f"{start_time} -> {end_time} - Data collected\n"
                    f"Prices: {price_fetched_count} Fetched.\n"
                    f"Queued: {total_messages_queued} | Sent: {total_messages_sent}\n"
                    f"Active Users: {active_users_count}"
                )
                print(debug_message)
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)

                if current_time.minute % 60 == 0:
                    await reinitialize_pairs()

                user_message_counts = {}
                message_queue = []
                total_messages_queued = 0
                total_messages_sent = 0
                blocked_users = set()
                last_message_time = {}
                user_flood_timeout = {}

                now = datetime.now()
                delay = 60 - now.second - now.microsecond / 1000000.0
                await asyncio.sleep(delay)
            except Exception as e:
                error_message = f"An unexpected error occurred in the main loop: {e}"
                logger.error(error_message, exc_info=True)
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                await asyncio.sleep(2)

        print("Main loop stopped gracefully.")

    finally:
        # Отмена всех задач
        print("Cancelling polling tasks...")
        price_polling_task.cancel()
        debug_polling_task.cancel()

        with suppress(asyncio.CancelledError):
            await asyncio.gather(price_polling_task, debug_polling_task, return_exceptions=True)

        # Закрытие соединений ccxt
        print("Closing exchange connections...")
        await exchange_binance.close()
        await exchange_bybit.close()

        # Закрытие сессий ботов Telegram
        print("Closing bot sessions...")
        await price_bot.session.close()
        await debug_bot.session.close()

        # Остановка всех оставшихся задач
        all_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in all_tasks:
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*all_tasks, return_exceptions=True)

        print("All resources closed, shutting down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Program interrupted by user (Ctrl+C).")
