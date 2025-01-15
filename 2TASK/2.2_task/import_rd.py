import psycopg2
import csv
import os
import pathlib
from datetime import datetime

BASE_CSV_PATH = os.getenv('ETL_PATH')

# подключение к БД
conn = psycopg2.connect(
    dbname = os.getenv("DB_NAME_DWH"),
    user = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
    host = os.getenv("DB_HOST"),
    port = os.getenv("DB_PORT")
)
cursor = conn.cursor()


def import_product():
    file_path = f"{BASE_CSV_PATH}/2TASK/2.2_task/sources/product_info.csv"

    with open(file_path, mode='r', encoding='windows-1251') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок

        for row in reader:
            product_rk, product_name, effective_from_date, effective_to_date = row

            if effective_from_date == '2023-03-15':
                continue 

            # Вставка данных в таблицу
            cursor.execute("""
                INSERT INTO rd.product (
                    product_rk, product_name, effective_from_date, effective_to_date
                ) VALUES (%s, %s, %s, %s)
            """, (product_rk, product_name, effective_from_date, effective_to_date))

    conn.commit()

def import_deal_info():
    file_path = f"{BASE_CSV_PATH}/2TASK/2.2_task/sources/deal_info.csv"

    with open(file_path, mode='r', encoding='windows-1251') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок

        for row in reader:
            cursor.execute("""
                INSERT INTO rd.deal_info (
                    deal_rk, deal_num, deal_name, deal_sum, client_rk, account_rk,
                    agreement_rk, deal_start_date, department_rk, product_rk, deal_type_cd,
                    effective_from_date, effective_to_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, row)

    conn.commit()

def main():
    # Загрузка данных для rd.product
    import_product()

    # Загрузка данных для rd.deal_info
    import_deal_info()

def close_connection():
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
    finally:
        close_connection()
