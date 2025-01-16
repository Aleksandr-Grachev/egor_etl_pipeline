import psycopg2
import csv
import os
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

def log_start(v_start_time_log, process_name, rows_processed=None):
    cursor.execute("""
        INSERT INTO logs.ETL_LOG (PROCESS_NAME, LOG_DATE, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, DURATION)
        VALUES (%s, CURRENT_DATE, %s, NULL, 'Start', %s , NULL);
    """, (process_name, v_start_time_log, rows_processed))

def log_end(v_end_time_log, v_duration_log, status, process_name, rows_processed, error_message=None):
    cursor.execute("""
        UPDATE logs.ETL_LOG 
        SET END_TIME = %s, DURATION = %s, STATUS = %s, ROWS_PROCESSED = %s, ERROR_MESSAGE = %s
        WHERE PROCESS_NAME = %s 
          AND LOG_DATE = CURRENT_DATE 
          AND STATUS = 'Start';
    """, (v_end_time_log, v_duration_log, status, rows_processed, error_message, process_name))

def export_to_csv():
    try:
        cursor.execute('SELECT * FROM dm.dm_f101_round_f')
        rows = cursor.fetchall()  # все строки данных
        columns = [desc[0] for desc in cursor.description]  # названия колонок из 1 строки

        file_path = f"{BASE_CSV_PATH}/1TASK/4_task/101.csv"

        process_name = "export_f101"
        v_start_time_log = datetime.now()

        log_start(v_start_time_log, process_name, rows_processed=len(rows))

        # Запись в csv
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)  # заголовки
            writer.writerows(rows)  # данные

        v_end_time_log = datetime.now()
        v_duration_log = v_end_time_log - v_start_time_log

        log_end(v_end_time_log, v_duration_log, 'Success', process_name, rows_processed=len(rows))

        conn.commit()
        print(f"Данные успешно выгружены в CSV файл: {file_path}")
        
    except Exception as e:
        conn.rollback()
        error_message = str(e)
        log_end(datetime.now(), None, 'Failed', "export_f101", 0, error_message)
        print(f"Ошибка при экспорте данных: {e}")

def clear_table():

    cursor.execute('TRUNCATE TABLE dm.dm_f101_round_f_v2;')

def import_from_csv():
    file_path = f"{BASE_CSV_PATH}/1TASK/4_task/101.csv"

    process_name = "import_f101"
    v_start_time_log = datetime.now()

    try:
        log_start(v_start_time_log, process_name)

        rows_processed = 0

        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Пропускаем заголовок

            for row in reader:
                processed_row = [
                    (None if value == '' else (0 if value == '0' else value))  # преобразование пустых строк данных
                    for value in row
                ]

                cursor.execute("""
                    INSERT INTO dm.dm_f101_round_f_v2 (
                        from_date, to_date, chapter, ledger_account, characteristic,
                        balance_in_rub, r_balance_in_rub, balance_in_val, r_balance_in_val,
                        balance_in_total, r_balance_in_total, turn_deb_rub, r_turn_deb_rub,
                        turn_deb_val, r_turn_deb_val, turn_deb_total, r_turn_deb_total,
                        turn_cre_rub, r_turn_cre_rub, turn_cre_val, r_turn_cre_val,
                        turn_cre_total, r_turn_cre_total, balance_out_rub, r_balance_out_rub,
                        balance_out_val, r_balance_out_val, balance_out_total, r_balance_out_total
                    ) 
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """, processed_row)

                rows_processed += 1

        v_end_time_log = datetime.now()
        v_duration_log = v_end_time_log - v_start_time_log

        log_end(v_end_time_log, v_duration_log, 'Success', process_name, rows_processed)

        conn.commit()
        print(f"Данные успешно загружены в таблицу из файла: {file_path}")
    except Exception as e:
        conn.rollback()
        error_message = str(e)
        log_end(datetime.now(), None, 'Failed', process_name, 0, error_message)
        print(f"Ошибка при импорте данных: {e}")

def main():

    action = input("Выберите действие (1 - экспорт в CSV, 2 - импорт из CSV): ")
    if action == '1':
        export_to_csv()
    elif action == '2':
        clear_table()
        import_from_csv()
    else:
        print("Неверный выбор! Попробуйте снова.")

def close_connection():
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
    finally:
        close_connection()