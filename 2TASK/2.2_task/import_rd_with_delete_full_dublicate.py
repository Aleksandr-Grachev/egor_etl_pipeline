# Удаление полных дубликатов по всем столбцам из rd.PRODUCT
import psycopg2
import csv
import os
import pathlib
from datetime import datetime

BASE_CSV_PATH = os.getenv('ETL_PATH')

# подключение к БД
conn = psycopg2.connect(
    dbname = os.getenv("DB_NAME"),
    user = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
    host = os.getenv("DB_HOST"),
    port = os.getenv("DB_PORT")
)
cursor = conn.cursor()


def import_product():
    file_path = f"{BASE_CSV_PATH}/csv/task2.2/product_info.csv"

    with open(file_path, mode='r', encoding='windows-1251') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок

        for row in reader:
            product_rk, product_name, effective_from_date, effective_to_date = row

            # проверка на полный дубликат
            cursor.execute("""
                SELECT 1 
                FROM rd.product 
                WHERE product_rk = %s
                  AND product_name = %s
                  AND effective_from_date = %s
                  AND effective_to_date = %s
            """, (product_rk, product_name, effective_from_date, effective_to_date))
            record_exists = cursor.fetchone()

            if not record_exists: # если не полный дубликат - вставляем
                cursor.execute("""
                    INSERT INTO rd.product (
                        product_rk, product_name, effective_from_date, effective_to_date
                    ) VALUES (%s, %s, %s, %s)
                """, (product_rk, product_name, effective_from_date, effective_to_date))

    conn.commit()


def import_deal_info():
    file_path = f"{BASE_CSV_PATH}/csv/task2.2/deal_info.csv"

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
    cursor.execute("TRUNCATE TABLE rd.product")
    import_product()

    import_deal_info()

def close_connection():
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
    finally:
        close_connection()
