import sqlite3

try:
    # Подключаемся к базе данных
    conn = sqlite3.connect('databases_modified_whitelist_2/whitelist.db')
    cursor = conn.cursor()

    # Добавляем новые столбцы с указанными значениями по умолчанию
    cursor.execute('ALTER TABLE whitelist ADD COLUMN OIperiod INTEGER DEFAULT 5')
    cursor.execute('ALTER TABLE whitelist ADD COLUMN OIpercent REAL DEFAULT 10.0')

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
