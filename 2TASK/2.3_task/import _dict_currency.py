import psycopg2
import csv
from datetime import datetime

BASE_PATH = r"C:\Users\bokla\ETL_pipeline\2TASK\2.3_task"

# подключение к БД
conn = psycopg2.connect(
    dbname="dwh",
    user="postgres",
    password="2002",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()


def import_dict_curency():
    file_path = f"{BASE_PATH}/dict_currency.csv"

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок

        for row in reader:
            currency_cd, currency_name, effective_from_date, effective_to_date = row

            # Вставка данных в таблицу
            cursor.execute("""
                INSERT INTO dm.dict_currency (
                    currency_cd, currency_name, effective_from_date, effective_to_date
                ) VALUES (%s, %s, %s, %s)
            """, (currency_cd, currency_name, effective_from_date, effective_to_date))

    conn.commit()

def main():
    # Загрузка данных для dm.dict_currency
    cursor.execute("TRUNCATE TABLE dm.dict_currency")
    import_dict_curency()

def close_connection():
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
    finally:
        close_connection()