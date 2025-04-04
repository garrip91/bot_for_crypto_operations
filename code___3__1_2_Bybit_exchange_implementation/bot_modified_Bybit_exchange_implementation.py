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
from dotenv import load_dotenv
import os


load_dotenv()

# Инициализация бирж
binance_exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}  # USDT-M Futures для Binance
})

bybit_exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'linear'}  # USDT Perpetual Futures для Bybit
})

# Структуры данных для хранения цен и OI, разделенные по биржам
prices = {'binance': {}, 'bybit': {}}  # Цены: {exchange: {pair: [price_list]}}
prices_cooldown = {'binance': {}, 'bybit': {}}  # Cooldown для цен: {exchange: {pair: {chat_id: {'Short': n, 'Dump': n}}}}
open_interest = {'binance': {}, 'bybit': {}}  # Открытый интерес: {exchange: {pair: [oi_list]}}
oi_cooldown = {'binance': {}, 'bybit': {}}  # Cooldown для OI: {exchange: {pair: {chat_id: {'OI': n}}}}

# Токены и настройки из .env
PRICE_TELEGRAM_TOKEN = os.getenv("PRICE_TELEGRAM_TOKEN")
DEBUG_BOT_TOKEN = os.getenv("DEBUG_BOT_TOKEN")
DEBUG_CHAT_ID = os.getenv("DEBUG_CHAT_ID")

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    filename='LOGS.log',  # Укажите здесь путь к вашему файлу логов
    filemode='a'  # 'a' - append (дописывать), 'w' - overwrite (перезаписывать)
)
logger = logging.getLogger(__name__)

# Пути к базам данных
WHITELIST_DB_PATH = "../databases_modified_whitelist/whitelist.db"
BAN_PAIRS_DB_PATH = "/var/www/site/payment/ban_pairs.db"

# Глобальные константы и переменные
GLOBAL_MESSAGES_PER_SECOND = 30
USER_MESSAGES_PER_MINUTE = 15
message_queue = []  # Очередь сообщений для отправки
user_message_counts = {}  # Счетчик сообщений по пользователям
total_messages_queued = 0  # Общее количество поставленных в очередь сообщений
total_messages_sent = 0  # Общее количество отправленных сообщений
blocked_users = set()  # Множество заблокированных пользователей
last_message_time = {}  # Время последнего сообщения для каждого chat_id
user_flood_timeout = {}  # Таймауты для пользователей из-за flood control
blocked_user_ids_forbidden = set()  # Пользователи, заблокировавшие бота
prices_lock = asyncio.Lock()  # Блокировка для асинхронного доступа к ценам
notification_counters = defaultdict(lambda: defaultdict(int))  # Счетчики уведомлений: {chat_id: {pair: count}}
last_counter_reset_date = datetime.now().date()  # Дата последнего сброса счетчиков
fetch_errors = []  # Список ошибок при получении данных
last_error_message_time = 0  # Время последнего сообщения об ошибке
ERROR_MESSAGE_INTERVAL = 60  # Интервал между сообщениями об ошибках (сек)

# Инициализация ботов и диспетчеров
price_bot = Bot(token=PRICE_TELEGRAM_TOKEN)
debug_bot = Bot(token=DEBUG_BOT_TOKEN)
price_dp = Dispatcher()
debug_dp = Dispatcher()
price_router = Router()
debug_router = Router()

# Хранилища данных
bot_data = {}  # Настройки пользователей: {chat_id: {settings}}
user_data = {}  # Временные данные пользователей: {chat_id: {awaiting: setting_type}}


# Декоратор для обработки ошибок Telegram
def telegram_error_handler(handler):
    @functools.wraps(handler)
    async def wrapper(*args, **kwargs):
        try:
            kwargs_copy = kwargs.copy()
            if 'dispatcher' in kwargs_copy:
                del kwargs_copy['dispatcher']
            sig = inspect.signature(handler)
            param_names = set(p.name for p in sig.parameters.values())
            filtered_kwargs = {k: v for k, v in kwargs_copy.items() if k in param_names}
            return await handler(*args, **filtered_kwargs)
        except Exception as e:
            message = next((arg for arg in args if isinstance(arg, Message)), None)
            chat_id = message.chat.id if message else "Unknown"
            error_message = f"Error in {handler.__name__} with chat_id {chat_id}: {e}"
            print(error_message)
            traceback.print_exc()
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
            return None
    return wrapper


# Асинхронная обработка ошибок
async def async_error_handler(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except Exception as e:
        message = next((arg for arg in args if isinstance(arg, Message)), None)
        chat_id = message.chat.id if message else "Unknown"
        error_message = f"Error in {func.__name__} with chat_id {chat_id}: {e}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


# Декоратор для повторных попыток при таймаутах
def global_timeout_retry(retries=3, delay=5):
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                kwargs_copy = kwargs.copy()
                if 'dispatcher' in kwargs_copy:
                    del kwargs_copy['dispatcher']
                sig = inspect.signature(func)
                param_names = set(p.name for p in sig.parameters.values())
                filtered_kwargs = {k: v for k, v in kwargs_copy.items() if k in param_names}
                for attempt in range(retries):
                    try:
                        return await func(*args, **filtered_kwargs)
                    except Exception as e:
                        if 'TimedOut' in str(e):
                            print(f"Telegram TimedOut error in async {func.__name__}: {e}. Retrying in {delay} seconds...")
                            await asyncio.sleep(delay)
                        else:
                            print(f"Unexpected error in async {func.__name__}: {e}. No retry.")
                            break
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                for attempt in range(retries):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if 'TimedOut' in str(e):
                            print(f"Telegram TimedOut error in {func.__name__}: {e}. Retrying in {delay} seconds...")
                            time.sleep(delay)
                        else:
                            print(f"Unexpected error in {func.__name__}: {e}. No retry.")
                            break
            return sync_wrapper
    return decorator


# Получение списка игнорируемых пар из базы данных
def get_ignored_pairs():
    db_path = BAN_PAIRS_DB_PATH
    ignored_pairs = []
    try:
        db = sqlite3.connect(db_path)
        cursor = db.cursor()
        cursor.execute('SELECT pair FROM ban')
        rows = cursor.fetchall()
        ignored_pairs = [row[0] for row in rows]
        db.close()
    except sqlite3.Error as e:
        error_message = f"Database error in get_ignored_pairs: {e}"
        print(error_message)
        asyncio.create_task(debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message))
    return ignored_pairs


# Функция для получения пар и цен с биржи
async def fetch_pairs_and_prices(exchange, exchange_name):
    
    markets = await exchange.load_markets()
    logger.info(f"{exchange_name}: Loaded {len(markets)} markets")
    
    usdt_perpetual = {
        symbol: market for symbol, market in markets.items()
        if (
            market.get('quote') == 'USDT' and
            market.get('contract') is True and
            market.get('option') is False and
            market.get('expiry') is None and
            market.get('linear') is True
        )
    }
    logger.info(f"{exchange_name}: Filtered {len(usdt_perpetual)} USDT perpetual pairs")
    
    # Убираем нормализацию для Binance, оставляем символы как есть
    normalized_symbols = list(usdt_perpetual.keys())
    logger.info(f"{exchange_name}: Symbols: {len(normalized_symbols)} (first 5: {normalized_symbols[:5]})")
    
    tickers = await exchange.fetch_tickers(normalized_symbols)
    logger.info(f"{exchange_name}: Fetched {len(tickers)} tickers")
    
    # Логируем структуру первого тикера для проверки
    if tickers:
        first_symbol = next(iter(tickers))
        logger.info(f"{exchange_name}: Ticker sample for {first_symbol}: {tickers[first_symbol]}")
    
    # Используем 'last' для обеих бирж, так как оно работает с правильными символами
    result = {symbol: tickers.get(symbol, {}).get('last') for symbol in normalized_symbols if tickers.get(symbol, {}).get('last') is not None}
    logger.info(f"{exchange_name}: Final pairs with prices: {len(result)}")
    
    return result


# Инициализация начальных цен и OI
@global_timeout_retry(retries=3, delay=5)
async def price_fetch_initial_prices():
    
    global prices, open_interest
    # Записываем время начала инициализации для логов
    start_time = datetime.now().strftime("%H:%M:%S")
    
    # Получаем список пар, которые нужно игнорировать из базы данных
    ignored_pairs = get_ignored_pairs()
    
    # Проходим по каждой бирже (Binance и Bybit)
    for exchange, ex_obj in [('binance', binance_exchange), ('bybit', bybit_exchange)]:
        # Получаем пары и их начальные цены с учетом фильтрации
        prices_data = await fetch_pairs_and_prices(ex_obj, exchange)
        
        # Инициализируем prices для данной биржи, исключая игнорируемые пары
        prices[exchange] = {pair: [price] for pair, price in prices_data.items() if pair not in ignored_pairs}
        
        # Блокируем доступ к ценам для безопасного обновления
        async with prices_lock:
            # Для каждой пары получаем начальный открытый интерес (OI)
            for pair in list(prices[exchange].keys()):
                try:
                    # Запрашиваем OI для данной пары
                    oi_data = await ex_obj.fetch_open_interest(pair)
                    # Если OI есть, сохраняем его, иначе ставим 0
                    open_interest[exchange][pair] = [oi_data['openInterest']] if oi_data.get('openInterest') else [0]
                except ccxt.ExchangeError as e:
                    # Обрабатываем ошибку -4108 (пара в доставке/расчетах)
                    if '-4108' in str(e):
                        logger.warning(f"Removing {pair} from {exchange} due to delivery/settlement: {e}")
                        # Удаляем проблемную пару из отслеживания
                        del prices[exchange][pair]
                    else:
                        # Логируем другие ошибки с OI для дальнейшего анализа
                        logger.error(f"Error fetching OI for {pair} on {exchange}: {e}")
    
    # Записываем время окончания инициализации
    end_time = datetime.now().strftime("%H:%M:%S")
    
    # Формируем сообщение с итогами инициализации
    summary_message = (
        f"{start_time} -> {end_time} - Initial prices collected\n"
        f"Binance: {len(prices['binance'])} Fetched\n"  # Количество пар для Binance
        f"Bybit: {len(prices['bybit'])} Fetched"  # Количество пар для Bybit
    )
    print(summary_message)
    # Отправляем итоги в дебаг-чат
    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)


# Обновление цен и OI
@global_timeout_retry(retries=3, delay=5)
async def price_fetch_and_compare_prices():
    
    try:
        # Счетчик успешно обновленных пар для каждой биржи
        fetched_count = {'binance': 0, 'bybit': 0}
        # Список пар с проблемами для логирования
        problem_pairs = {'binance': [], 'bybit': []}

        # Проходим по каждой бирже
        for exchange, ex_obj in [('binance', binance_exchange), ('bybit', bybit_exchange)]:
            # Получаем список отслеживаемых пар для данной биржи
            tracked_pairs = list(prices[exchange].keys())
            if not tracked_pairs:
                continue  # Пропускаем, если нет пар для обработки

            # Получаем текущие цены для всех пар разом
            tickers = await ex_obj.fetch_tickers(tracked_pairs)
            
            # Блокируем доступ к данным для безопасного обновления
            async with prices_lock:
                for pair in tracked_pairs:
                    try:
                        # Получаем новую цену для пары
                        new_price = tickers.get(pair, {}).get('last')
                        if new_price is not None:
                            # Добавляем новую цену в начало списка
                            prices[exchange][pair].insert(0, new_price)
                            # Ограничиваем длину списка до 30 значений
                            if len(prices[exchange][pair]) > 30:
                                prices[exchange][pair].pop()
                            fetched_count[exchange] += 1  # Увеличиваем счетчик успешных обновлений
                    
                        # Обновляем открытый интерес
                        try:
                            oi = await ex_obj.fetch_open_interest(pair)
                            if oi.get('openInterest') is not None:
                                # Добавляем новый OI в начало списка
                                open_interest[exchange][pair].insert(0, oi['openInterest'])
                                # Ограничиваем длину списка до 30 значений
                                if len(open_interest[exchange][pair]) > 30:
                                    open_interest[exchange][pair].pop()
                            else:
                                # Логируем, если OI отсутствует в ответе
                                logger.warning(f"No openInterest data for {pair} on {exchange}")
                        except ccxt.ExchangeError as e:
                            # Обрабатываем ошибку -4108
                            if '-4108' in str(e):
                                logger.warning(f"Skipping {pair} on {exchange} due to delivery/settlement: {e}")
                                problem_pairs[exchange].append(pair)  # Добавляем в список проблемных
                            else:
                                # Логируем другие ошибки с OI
                                logger.error(f"Error fetching OI for {pair} on {exchange}: {e}")
                                problem_pairs[exchange].append(pair)
                        
                    except Exception as e:
                        # Логируем любые другие ошибки обработки пары
                        logger.error(f"Error processing {pair} on {exchange}: {e}")
                        problem_pairs[exchange].append(pair)
        
        # Если были проблемные пары, формируем сообщение для логов
        if any(problem_pairs.values()):
            error_details = (
                f"Skipped pairs due to issues:\n"
                f"Binance: {', '.join(problem_pairs['binance']) or 'None'}\n"  # Проблемные пары Binance
                f"Bybit: {', '.join(problem_pairs['bybit']) or 'None'}"  # Проблемные пары Bybit
            )
            print(error_details)
            fetch_errors.append(error_details)  # Добавляем в глобальный список ошибок
        
        # Возвращаем количество успешно обновленных пар
        return fetched_count
    
    except Exception as e:
        # Обработка глобальных ошибок (например, проблемы с сетью)
        error_details = (
            f"Error fetching bulk prices:\n"
            f"{e.__class__.__name__}: {str(e)}\n"
            f"{traceback.format_exc()}"
        )
        print(error_details)
        fetch_errors.append(error_details)
        return {'binance': 0, 'bybit': 0}  # В случае ошибки возвращаем 0


# Реинициализация пар
@global_timeout_retry(retries=3, delay=5)
async def reinitialize_pairs():
    global prices, open_interest
    start_time = datetime.now().strftime("%H:%M:%S")
    ignored_pairs = get_ignored_pairs()
    logger.info(f"Starting reinitialization. Ignored pairs: {len(ignored_pairs)}")
    
    for exchange, ex_obj in [('binance', binance_exchange), ('bybit', bybit_exchange)]:
        try:
            # Получаем новые пары и цены
            new_prices = await fetch_pairs_and_prices(ex_obj, exchange)
            logger.info(f"{exchange}: Retrieved {len(new_prices)} pairs from fetch_pairs_and_prices")
            
            # Блокируем доступ к данным для безопасного обновления
            async with prices_lock:
                # Обновляем prices, исключая игнорируемые пары
                prices[exchange] = {pair: [price] for pair, price in new_prices.items() if pair not in ignored_pairs}
                logger.info(f"{exchange}: After filtering ignored pairs: {len(prices[exchange])}")
                
                # Обновляем OI для каждой пары
                for pair in list(prices[exchange].keys()):
                    try:
                        oi_data = await ex_obj.fetch_open_interest(pair)
                        open_interest[exchange][pair] = [oi_data['openInterest']] if oi_data.get('openInterest') else [0]
                    except ccxt.ExchangeError as e:
                        if '-4108' in str(e):
                            logger.warning(f"Removing {pair} from {exchange} due to delivery/settlement: {e}")
                            del prices[exchange][pair]
                        else:
                            logger.error(f"Error fetching OI for {pair} on {exchange}: {e}")
                
                # Очистка cooldown для удаленных пар
                for pair in list(prices_cooldown[exchange].keys()):
                    if pair not in prices[exchange]:
                        del prices_cooldown[exchange][pair]
                for pair in list(oi_cooldown[exchange].keys()):
                    if pair not in prices[exchange]:
                        del oi_cooldown[exchange][pair]
                logger.info(f"{exchange}: Final count after OI and cooldown: {len(prices[exchange])}")
        
        except Exception as e:
            logger.error(f"Failed to reinitialize {exchange}: {e}")
            prices[exchange] = {}
            open_interest[exchange] = {}
    
    end_time = datetime.now().strftime("%H:%M:%S")
    summary_message = (
        f"{start_time} -> {end_time} - Re-initialization done\n"
        f"Binance: {len(prices['binance'])} Fetched\n"
        f"Bybit: {len(prices['bybit'])} Fetched"
    )
    print(summary_message)
    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)


# Проверка статуса пользователя
def is_user_whitelisted_and_active(chat_id):
    db = sqlite3.connect(WHITELIST_DB_PATH)
    cursor = db.cursor()
    cursor.execute('SELECT Binance, Bybit, Blocked FROM whitelist WHERE TelegramID = ? AND Active = 1', (chat_id,))
    result = cursor.fetchone()
    db.close()
    return result if result else (0, 0, 1)  # По умолчанию отключено и заблокировано, если нет записи


# Отправка уведомления
@global_timeout_retry(retries=3, delay=5)
async def price_send_alert(exchange, pair, change_percent, old_value, new_value, value_list, condition_type, settings, chat_id, is_oi=False):
    global message_queue, total_messages_queued, notification_counters
    exchange_emojis = {'binance': '💎', 'bybit': '🌙'}
    emoji = exchange_emojis[exchange]
    
    if is_oi:
        signal_name = 'OI Change'
        period = f"{settings['oi_period']} min"
        value_type = 'OI'
    else:
        value_type = 'Price'
        if condition_type == 'Short':
            signal_name = 'Pump Signal'
            period = f"{settings['pump_index']} min"
        elif condition_type == 'Dump':
            signal_name = 'Dump Signal'
            period = f"{settings['dump_index']} min"
    
    raw_symbol = pair.replace(':USDT', '').replace('/', '')
    url_symbol = raw_symbol
    pair_display = raw_symbol.replace('USDT', '')
    url = f"https://www.tradingview.com/chart/?symbol={exchange.upper()}%3A{url_symbol}.P"
    hyperlink = f'<a href="{url}">{pair_display}</a>'
    
    formatted_old_value = f"{old_value:.8f}".rstrip('0').rstrip('.')
    formatted_new_value = f"{new_value:.8f}".rstrip('0').rstrip('.')
    alert_number = notification_counters[chat_id][pair] + 1
    
    message = (
        f"{emoji} <b>{hyperlink}</b> | {value_type} {signal_name}\n"
        f"{emoji} {exchange.capitalize()} | {period}\n"
        f"{value_type} Change: <b>{abs(change_percent):.2f}%</b>\n"
        f"{formatted_old_value} -> <b>{formatted_new_value}</b>\n"
        f"🔇 Alert Number: <b>{alert_number}</b>"
    )
    message_queue.append((chat_id, message))
    total_messages_queued += 1


# Проверка изменений и отправка уведомлений
async def price_check_and_send_notifications():
    global notification_counters, last_counter_reset_date
    current_date = datetime.now().date()
    if current_date != last_counter_reset_date:
        notification_counters = defaultdict(lambda: defaultdict(int))
        last_counter_reset_date = current_date
    
    for exchange in ['binance', 'bybit']:
        async with prices_lock:
            for pair, price_list in prices[exchange].items():
                if pair not in prices_cooldown[exchange]:
                    prices_cooldown[exchange][pair] = {}
                    oi_cooldown[exchange][pair] = {}
                
                for chat_id, settings in bot_data.items():
                    binance_enabled, bybit_enabled, blocked = is_user_whitelisted_and_active(chat_id)
                    if blocked or (exchange == 'binance' and not binance_enabled) or (exchange == 'bybit' and not bybit_enabled):
                        continue
                    
                    alert_limit = settings.get('alert_limit', 20)
                    notifications_sent = notification_counters[chat_id][pair]  # Исправлено: убраны скобки после chat_id
                    if alert_limit is not None and notifications_sent >= alert_limit:
                        continue
                    
                    # Инициализация cooldown для пользователя
                    if chat_id not in prices_cooldown[exchange][pair]:
                        prices_cooldown[exchange][pair][chat_id] = {'Short': 0, 'Dump': 0}
                    if chat_id not in oi_cooldown[exchange][pair]:
                        oi_cooldown[exchange][pair][chat_id] = {'OI': 0}
                    
                    # Уменьшение cooldown
                    for condition in ['Short', 'Dump']:
                        if prices_cooldown[exchange][pair][chat_id][condition] > 0:
                            prices_cooldown[exchange][pair][chat_id][condition] -= 1
                    if oi_cooldown[exchange][pair][chat_id]['OI'] > 0:
                        oi_cooldown[exchange][pair][chat_id]['OI'] -= 1
                    
                    # Проверка цен
                    new_price = price_list[0]
                    pump_index = settings['pump_index']
                    pump_threshold = settings['pump_threshold']
                    if len(price_list) > pump_index and prices_cooldown[exchange][pair][chat_id]['Short'] == 0:
                        old_price = price_list[pump_index]
                        change_percent = (new_price - old_price) / old_price * 100
                        if change_percent >= pump_threshold:
                            await price_send_alert(exchange, pair, change_percent, old_price, new_price, price_list, 'Short', settings, chat_id)
                            prices_cooldown[exchange][pair][chat_id]['Short'] = pump_index
                            notification_counters[chat_id][pair] += 1  # Исправлено: убраны скобки после chat_id
                            continue
                    
                    d_index = settings['dump_index']
                    d_threshold = settings['dump_threshold']
                    if len(price_list) > d_index and prices_cooldown[exchange][pair][chat_id]['Dump'] == 0:
                        old_price = price_list[d_index]
                        change_percent = (new_price - old_price) / old_price * 100
                        if change_percent <= -d_threshold:
                            await price_send_alert(exchange, pair, change_percent, old_price, new_price, price_list, 'Dump', settings, chat_id)
                            prices_cooldown[exchange][pair][chat_id]['Dump'] = d_index
                            notification_counters[chat_id][pair] += 1  # Исправлено: убраны скобки после chat_id
                            continue  # Добавлен continue для избежания двойных уведомлений
                    
                    # Проверка OI (базовая реализация, будет доработана в п.1.1)
                    oi_list = open_interest[exchange].get(pair, [])
                    if len(oi_list) > settings.get('oi_period', 5) and oi_cooldown[exchange][pair][chat_id]['OI'] == 0:
                        old_oi = oi_list[settings['oi_period']]
                        new_oi = oi_list[0]
                        oi_change = (new_oi - old_oi) / old_oi * 100 if old_oi != 0 else 0
                        if abs(oi_change) >= settings.get('oi_threshold', 10):
                            await price_send_alert(exchange, pair, oi_change, old_oi, new_oi, oi_list, 'Change', settings, chat_id, is_oi=True)
                            oi_cooldown[exchange][pair][chat_id]['OI'] = settings['oi_period']
                            notification_counters[chat_id][pair] += 1  # Исправлено: убраны скобки после chat_id


# Отправка сообщения пользователю
async def send_message(chat_id, message):
    global total_messages_sent, last_message_time, user_flood_timeout, blocked_user_ids_forbidden
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
            match = re.search(r'retry_after=(\d+)', error_str)
            retry_after = int(match.group(1)) if match else 30
            print(f"Flood control exceeded for chat_id {chat_id}. Retry in {retry_after} seconds.")
            user_flood_timeout[chat_id] = time.time() + retry_after
            message_queue.append((chat_id, message))
        elif "bot was blocked by the user" in error_str.lower():
            blocked_user_ids_forbidden.add(chat_id)
        else:
            error_message = f"Error.Concurrent sending to {chat_id}: {error_str}"
            print(error_message)
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


# Обработка очереди сообщений
async def process_message_queue():
    global message_queue, user_message_counts, blocked_users, user_flood_timeout, blocked_user_ids_forbidden
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
    
    if blocked_user_ids_forbidden:
        summary_message = (
            "The following users have blocked the bot:\n"
            + ", ".join(str(uid) for uid in blocked_user_ids_forbidden)
        )
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=summary_message)
        blocked_user_ids_forbidden.clear()


# Загрузка данных пользователей из базы
def load_user_data():
    try:
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()
        cursor.execute("SELECT TelegramID, Pindex, Ppercent, Dindex, Dpercent, Filter, Binance, Bybit, Blocked FROM whitelist WHERE Active = 1")
        rows = cursor.fetchall()
        for row in rows:
            telegram_id = int(row[0])
            p_index = row[1]
            p_percent = row[2]
            d_index = row[3]
            d_percent = row[4]
            alert_limit = row[5] if row[5] is not None else 100
            binance = row[6]
            bybit = row[7]
            blocked = row[8]
            bot_data[telegram_id] = {
                'pump_index': p_index,
                'pump_threshold': p_percent,
                'dump_index': d_index,
                'dump_threshold': d_percent,
                'alert_limit': alert_limit,
                'binance': binance,
                'bybit': bybit,
                'blocked': blocked,
                'oi_period': 5,  # Значение по умолчанию, будет доработано в п.1.1
                'oi_threshold': 10  # Значение по умолчанию, будет доработано в п.1.1
            }
        db.close()
        print(f"Loaded user data for {len(rows)} users.")
    except sqlite3.Error as e:
        error_message = f"Database error in load_user_data: {e}"
        print(error_message)
        asyncio.create_task(debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message))


# Обработчик команды /start
@price_router.message(CommandStart())
async def price_start(message: Message):
    try:
        chat_id = message.chat.id
        user = message.from_user
        username = user.username or ''
        args = message.text.split()[1:] if message.text.split() else []
        referral_code = args[0] if args else None
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()
        cursor.execute('SELECT Active, StartDate, EndDate, Pindex, Ppercent, Dindex, Dpercent, Binance, Bybit, Blocked FROM whitelist WHERE TelegramID = ?', (chat_id,))
        result = cursor.fetchone()
        
        if result:
            active, start_date_db, end_date_db, p_index, p_percent, d_index, d_percent, binance, bybit, blocked = result
            is_new_user = False
            start_date = start_date_db
            end_date = end_date_db
        else:
            trial_days = 30
            start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            end_date = (datetime.now() + timedelta(days=trial_days)).strftime('%Y-%m-%d %H:%M:%S')
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
        
        bot_data[chat_id] = {
            'pump_index': p_index,
            'pump_threshold': p_percent,
            'dump_index': d_index,
            'dump_threshold': d_percent,
            'alert_limit': 100,
            'binance': binance,
            'bybit': bybit,
            'blocked': blocked,
            'oi_period': 5,
            'oi_threshold': 10
        }
        
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
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Bot Settings"), KeyboardButton(text="Payment Settings")],
                [KeyboardButton(text="Contact Support")]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await message.reply(welcome_message, parse_mode='HTML', reply_markup=keyboard)
        
        if is_new_user:
            trial_message = (
                f"<b>Your Trial period has started and lasts for {trial_days} days.</b>\n"
                f"<b>It will expire at {end_date}.</b>\n"
                "Check more details by pressing <b>Payment Settings -> Check Profile</b>.\n"
                "<b>Good luck with your trading!</b>"
            )
            await message.reply(trial_message, parse_mode='HTML')
        
        current_time = datetime.now().strftime("%H:%M:%S")
        debug_message = f"{current_time} New user {chat_id} added or updated"
        print(debug_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)
        db.close()
    except sqlite3.Error as e:
        error_message = f"Database error in price_start: {e}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


# Показать настройки бота
@price_router.message(F.text == "Bot Settings")
async def price_show_bot_settings(message: Message):
    try:
        chat_id = message.chat.id
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()
        cursor.execute('SELECT Active FROM whitelist WHERE TelegramID = ?', (chat_id,))
        status_check = cursor.fetchone()
        active = int(status_check[0]) if status_check else 0
        cursor.execute('SELECT Pindex, Ppercent, Dindex, Dpercent, Filter FROM whitelist WHERE TelegramID = ?', (chat_id,))
        user_settings = cursor.fetchone()
        
        if user_settings:
            p_index, p_percent, d_index, d_percent, alert_limit = user_settings
            alert_limit = alert_limit if alert_limit is not None else 100
        else:
            p_index, p_percent = 3, 5
            d_index, d_percent = 2, 8
            alert_limit = 100
        
        bot_data[chat_id] = {
            'pump_index': p_index,
            'pump_threshold': p_percent,
            'dump_index': d_index,
            'dump_threshold': d_percent,
            'alert_limit': alert_limit,
            'oi_period': 5,
            'oi_threshold': 10
        }
        
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
        second_message = (
            f"<b>Your current notification settings are:</b>\n\n"
            f"🟢 Pump Period: <b>{p_index}</b> min\n"
            f"➗ Pump Percentage: <b>{p_percent}%</b>\n\n"
            f"🔴 Dump Period: <b>{d_index}</b> min\n"
            f"➗ Dump Percentage: <b>{d_percent}%</b>\n\n"
            f"🔔 Alert Limit: <b>{'Not set' if alert_limit == 100 else ('Unlimited' if alert_limit is None else f'{alert_limit} per day')}</b>"
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


# Показать настройки оплаты
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


# Обработка оплаты
@price_router.message(F.text == "Make a payment")
async def price_make_payment(message: Message):
    chat_id = message.chat.id
    current_time = datetime.now().strftime("%H:%M:%S")
    logger.info(f"{current_time} - Payment initiated - Chat ID: {chat_id}")
    debug_message = f"{current_time} - Payment initiated - Chat ID: {chat_id}"
    await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)
    
    first_message = (
        "⭐️ By making a payment, you will gain access to the <b>Pump Bot</b>. "
        "We offer the following plans:\n\n"
        "1️⃣<b>$Price1 – 1 month of access</b>\n"
        "2️⃣<b>$Price2   – 2 months of access</b>\n"
        "3️⃣<b>$Price3  – 3 months of access</b>\n\n"
        "You can pay using the following options: Tether (TRC20, ERC20), Bitcoin, Ethereum, and Litecoin.\n\n"
        "❓<b>How to Pay:</b>\n\n"
        "1. Once you choose payment plan and press corresponding button, you will be redirected to an invoice page where you can choose your preferred payment method and network. Once selected, click <b>\"Pay\"</b>.\n\n"
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
    
    base_url_1 = f"http://opportunity-trading.online/...?{chat_id}"
    base_url_2 = f"http://opportunity-trading.online/...?{chat_id}"
    base_url_3 = f"http://opportunity-trading.online/...?{chat_id}"
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="Price = 1 month", url=base_url_1))
    builder.add(InlineKeyboardButton(text="Price = 2 months", url=base_url_2))
    builder.add(InlineKeyboardButton(text="Price = 3 months", url=base_url_3))
    builder.adjust(1)
    await message.reply("Click the button below to generate a payment invoice:", reply_markup=builder.as_markup())


# Проверка профиля
@price_router.message(F.text == "Check profile")
async def price_check_profile(message: Message):
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    try:
        db = sqlite3.connect(WHITELIST_DB_PATH)
        cursor = db.cursor()
        cursor.execute('SELECT Active, EndDate, Referral FROM whitelist WHERE TelegramID = ?', (chat_id,))
        result = cursor.fetchone()
        if result:
            active, end_date, referral = result
            referral_display = referral if referral else "None"
            if active == 1:
                profile_message = (
                    f"👨‍💻User name: {user_name}\n"
                    f"🆔User ID: {chat_id}\n"
                    f"🤝Referral Code: {referral_display}\n"
                    f"🕔Access until: {end_date}"
                )
            else:
                profile_message = (
                    f"👨‍💻User name: {user_name}\n"
                    f"🆔User ID: {chat_id}\n"
                    f"🤝Referral Code: {referral_display}\n"
                    "⚠️<b>Access denied. Please make a payment in order to access Bot's functionality</b>⚠️"
                )
        else:
            profile_message = "No profile information found."
        await message.reply(profile_message, parse_mode='HTML')
        db.close()
    except sqlite3.Error as e:
        error_message = f"Database error in price_check_profile: {e}"
        print(error_message)
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)


# Обработка кнопки "Back"
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


# Обработка кнопки "Cancel"
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


# Обработка кнопки "Contact Support"
@price_router.message(F.text == "Contact Support")
async def price_support(message: Message):
    support_url = 'https://t.me/opportunity_trading'
    await message.reply(
        "💬 <b>Do you have any questions?</b>\n"
        "Then please do not hesitate to ask! You can contact us through "
        f"<a href='{support_url}'>this chat</a>.\n\n"
        "<b>We will reply as soon as possible. </b>",
        parse_mode='HTML',
        disable_web_page_preview=True
    )


# Отправка сообщения всем пользователям (debug)
@debug_router.message(Command("send_message"))
async def price_handle_send_message(message: Message):
    chat_id = message.chat.id
    message_to_send = message.text.replace("/send_message", "", 1).strip()
    if not message_to_send:
        await message.reply("Don't forget a text. Format: /send_message *text*")
        return
    for user_chat_id in bot_data.keys():
        try:
            await price_bot.send_message(chat_id=user_chat_id, text=message_to_send)
        except Exception as e:
            error_message = f"⚠️ Failed to send message to {user_chat_id}: {e} ⚠️"
            print(error_message)
            await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
    await message.reply("Message sent.")


# Ожидание ввода Pump Period
@price_router.message(F.text == "🟢 Pump Period")
@telegram_error_handler
async def awaiting_pump_index(message: Message):
    chat_id = message.chat.id
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


# Ожидание ввода Pump Percentage
@price_router.message(F.text == "➗ Pump Percentage")
@telegram_error_handler
async def awaiting_pump_threshold(message: Message):
    chat_id = message.chat.id
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


# Ожидание ввода Dump Period
@price_router.message(F.text == "🔴 Dump Period")
@telegram_error_handler
async def awaiting_dump_index(message: Message):
    chat_id = message.chat.id
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


# Ожидание ввода Dump Percentage
@price_router.message(F.text == "➗ Dump Percentage")
@telegram_error_handler
async def awaiting_dump_threshold(message: Message):
    chat_id = message.chat.id
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


# Ожидание ввода Alert Limit
@price_router.message(F.text == "🔔 Alert Limit")
@telegram_error_handler
async def awaiting_alert_limit(message: Message):
    chat_id = message.chat.id
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


# Установка предпочтений пользователя
@price_router.message(lambda message: message.text and not message.text.startswith('/') and not message.text in [
    "Bot Settings", "Payment Settings", "Contact Support", "Make a payment", "Check profile", "Back", "Cancel",
    "🟢 Pump Period", "➗ Pump Percentage", "🔴 Dump Period", "➗ Dump Percentage", "🔔 Alert Limit"
])
@telegram_error_handler
async def price_set_pref(message: Message):
    query = message.text
    chat_id = message.chat.id
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
    if setting_name == 'alert_limit' and query.lower() == 'all':
        value = None
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
    
    if chat_id in bot_data:
        bot_data[chat_id][setting_name] = value
    else:
        bot_data[chat_id] = {
            'pump_index': 2,
            'pump_threshold': 10,
            'dump_index': 2,
            'dump_threshold': 8,
            'alert_limit': 20,
            'oi_period': 5,
            'oi_threshold': 10
        }
        bot_data[chat_id][setting_name] = value
    
    db_column_map = {
        'pump_index': 'Pindex',
        'pump_threshold': 'Ppercent',
        'dump_index': 'Dindex',
        'dump_threshold': 'Dpercent',
        'alert_limit': 'Filter'
    }
    db_column = db_column_map[setting_name]
    db = sqlite3.connect(WHITELIST_DB_PATH)
    cursor = db.cursor()
    cursor.execute(f'UPDATE whitelist SET {db_column} = ? WHERE TelegramID = ?', (value, chat_id))
    db.commit()
    db.close()
    
    if chat_id in user_data:
        del user_data[chat_id]['awaiting']
    
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
    setting_display_name = {
        'pump_index': '🟢 Pump Period',
        'pump_threshold': '➗ Pump Percentage',
        'dump_index': '🔴 Dump Period',
        'dump_threshold': '➗ Dump Percentage',
        'alert_limit': '🔔 Alert Limit'
    }
    display_value = 'Unlimited' if (setting_name == 'alert_limit' and value is None) else f"{value}"
    await message.reply(
        f"<b>{setting_display_name[setting_name]} is set to {display_value}</b>",
        parse_mode='HTML',
        reply_markup=keyboard
    )


# Основной цикл программы
async def main():
    
    global user_message_counts, message_queue, total_messages_queued, total_messages_sent, blocked_users
    global last_message_time, user_flood_timeout, fetch_errors, last_error_message_time, notification_counters
    
    # Инициализация глобальных переменных
    user_message_counts = {}
    message_queue = []
    total_messages_queued = 0
    total_messages_sent = 0
    blocked_users = set()
    last_message_time = {}
    user_flood_timeout = {}
    fetch_errors = []
    last_error_message_time = 0
    notification_counters = defaultdict(lambda: defaultdict(int))  # Явно инициализируем здесь
    
    # Подключение роутеров
    price_dp.include_router(price_router)
    debug_dp.include_router(debug_router)
    
    # Удаление вебхуков и запуск polling
    await price_bot.delete_webhook(drop_pending_updates=True)
    await debug_bot.delete_webhook(drop_pending_updates=True)
    load_user_data()
    price_polling_task = asyncio.create_task(price_dp.start_polling(price_bot))
    debug_polling_task = asyncio.create_task(debug_dp.start_polling(debug_bot))
    
    # Синхронизация с началом минуты
    now = datetime.now()
    delay = 60 - now.second - now.microsecond / 1000000.0
    await asyncio.sleep(delay)
    await reinitialize_pairs()
    
    try:
        while True:
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
                
                # Получение количества активных пользователей
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
                    f"Binance Prices: {price_fetched_count['binance']} Fetched\n"
                    f"Bybit Prices: {price_fetched_count['bybit']} Fetched\n"
                    f"Queued: {total_messages_queued} | Sent: {total_messages_sent}\n"
                    f"Active Users: {active_users_count}"
                )
                print(debug_message)
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=debug_message)
                
                if current_time.minute % 60 == 0:
                    await reinitialize_pairs()
                
                # Сброс временных данных
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
                error_message = f"An unexpected error occurred in the main loop: {e}\nTraceback:\n{traceback.format_exc()}"
                logger.error(error_message)
                await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text=error_message)
                await asyncio.sleep(2)

    except KeyboardInterrupt:
        print("Bot interrupted by user, shutting down gracefully...")
        await debug_bot.send_message(chat_id=DEBUG_CHAT_ID, text="Bot interrupted by user, shutting down gracefully...")
    finally:
        price_polling_task.cancel()
        debug_polling_task.cancel()
        await binance_exchange.close()
        await bybit_exchange.close()


if __name__ == "__main__":
    asyncio.run(main())
