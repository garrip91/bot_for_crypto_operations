import sqlite3

try:
    # Подключаемся к базе данных
    conn = sqlite3.connect('databases_modified_whitelist/whitelist.db')
    cursor = conn.cursor()

    # Удаляем колонку Test
    cursor.execute('ALTER TABLE whitelist DROP COLUMN Test')

    # Добавляем колонку Binance с дефолтным значением 1
    cursor.execute('ALTER TABLE whitelist ADD COLUMN Binance INTEGER DEFAULT 1')

    # Добавляем колонку Bybit с дефолтным значением 1
    cursor.execute('ALTER TABLE whitelist ADD COLUMN Bybit INTEGER DEFAULT 1')

    # Добавляем колонку Blocked с дефолтным значением 0
    cursor.execute('ALTER TABLE whitelist ADD COLUMN Blocked INTEGER DEFAULT 0')

    # Сохраняем изменения
    conn.commit()
    print("Структура таблицы успешно обновлена")

except sqlite3.Error as e:
    print(f"Произошла ошибка: {e}")

finally:
    # Закрываем соединение
    if conn:
        conn.close()
        print("Соединение с базой данных закрыто")
