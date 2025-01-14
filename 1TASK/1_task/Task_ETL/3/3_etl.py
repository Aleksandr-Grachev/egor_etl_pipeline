import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import time

BASE_PATH = r"C:\Users\bokla\OneDrive\Рабочий стол\NeoStudy\csv"

 # Функция для логирования
def log_etl_process(connection, process_name, start_time, end_time, status, rows_processed, error_message=None):
     insert_log_query = """
     INSERT INTO logs.ETL_LOG (PROCESS_NAME, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, ERROR_MESSAGE)
     VALUES (:process_name, :start_time, :end_time, :status, :rows_processed, :error_message);
     """
     connection.execute(text(insert_log_query), {
         'process_name': process_name,
         'start_time': start_time,
         'end_time': end_time,
         'status': status,
         'rows_processed': rows_processed,
         'error_message': error_message
     })



 # Функция для загрузки FT_BALANCE_F
def load_ft_balance_f(connection, df):
     try:
         df['ON_DATE'] = pd.to_datetime(df['ON_DATE'], format='%d.%m.%Y').dt.date
         print("Данные успешно преобразованы для FT_BALANCE_F.")
     except Exception as e:
         print(f"Ошибка при преобразовании данных для FT_BALANCE_F: {e}")
         return


     update_query = """
     UPDATE ds.FT_BALANCE_F
     SET BALANCE_OUT = :balance_out,
         CURRENCY_RK = :currency_rk
     WHERE ON_DATE = :on_date AND ACCOUNT_RK = :account_rk;
     """


     insert_query = """
     INSERT INTO ds.FT_BALANCE_F (ON_DATE, ACCOUNT_RK, CURRENCY_RK, BALANCE_OUT)
     VALUES (:on_date, :account_rk, :currency_rk, :balance_out);
     """


     if df.empty:
         print("Ошибка: датафрейм пустой для FT_BALANCE_F. Нет данных для загрузки.")
         return

     start_time = datetime.now()
     process_name = "FT_BALANCE_F_Load"
     error_message = None

     time.sleep(5)

     try:
         with connection.begin():
             rows_processed = 0
             for _, row in df.iterrows():
                 # Обрабатываем значения, заменяем на None, если они отсутствуют
                 currency_rk = row['CURRENCY_RK'] if pd.notna(row['CURRENCY_RK']) else None
                 balance_out = row['BALANCE_OUT'] if pd.notna(row['BALANCE_OUT']) else None

                 data = {
                     'on_date': row['ON_DATE'],
                     'account_rk': row['ACCOUNT_RK'],
                     'currency_rk': currency_rk,
                     'balance_out': balance_out
                 }
                 select_query = """
                                 SELECT 1 FROM ds.FT_BALANCE_F
                                 WHERE ACCOUNT_RK = :account_rk AND ON_DATE = :on_date;
                                 """

                 result = connection.execute(text(select_query), data).fetchone()

                 if result:
                     connection.execute(text(update_query), data)
                 else:
                     connection.execute(text(insert_query), data)

                 rows_processed += 1

             end_time = datetime.now()
             status = "Success"
             log_etl_process(connection, process_name, start_time, end_time, status, rows_processed)

             print(f"Данные успешно загружены или обновлены в таблицу FT_BALANCE_F. Обработано {rows_processed} строк.")

     except Exception as e:
         end_time = datetime.now()
         status = "Failed"
         error_message = str(e)
         log_etl_process(connection, process_name, start_time, end_time, status, 0, error_message)
         print(f"Ошибка при обработке данных для FT_BALANCE_F: {e}")



 # Функция для загрузки FT_POSTING_F
def load_ft_posting_f(connection, df):
     try:
         df['OPER_DATE'] = pd.to_datetime(df['OPER_DATE'], format='%d-%m-%Y').dt.date
         print("Данные успешно преобразованы для FT_POSTING_F.")
     except Exception as e:
         print(f"Ошибка при преобразовании данных для FT_POSTING_F: {e}")
         return

     clear_query = """
     TRUNCATE TABLE ds.FT_POSTING_F;
     """

     insert_query = """
     INSERT INTO ds.FT_POSTING_F (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK, CREDIT_AMOUNT, DEBET_AMOUNT)
     VALUES (:oper_date, :credit_account_rk, :debet_account_rk, :credit_amount, :debet_amount);
     """

     if df.empty:
         print("Ошибка: датафрейм пустой для FT_POSTING_F. Нет данных для загрузки.")
         return

     start_time = datetime.now()
     process_name = "FT_POSTING_F_Load"
     error_message = None

     time.sleep(5)

     try:
         with connection.begin():
             connection.execute(text(clear_query))

             rows_processed = 0
             for _, row in df.iterrows():
                 # Проверка на пустые значения для обязательных полей (PK)
                 if pd.isna(row['CREDIT_ACCOUNT_RK']) or pd.isna(row['DEBET_ACCOUNT_RK']) or pd.isna(row['OPER_DATE']):
                     print(f"Пропущена строка с пустыми ключами: CREDIT_ACCOUNT_RK = {row['CREDIT_ACCOUNT_RK']}, DEBET_ACCOUNT_RK = {row['DEBET_ACCOUNT_RK']}, OPER_DATE = {row['OPER_DATE']}")
                     continue
                 credit_amount = row['CREDIT_AMOUNT'] if pd.notna(row['CREDIT_AMOUNT']) else None
                 debet_amount = row['DEBET_AMOUNT'] if pd.notna(row['DEBET_AMOUNT']) else None

                 data = {
                     'oper_date': row['OPER_DATE'],
                     'credit_account_rk': row['CREDIT_ACCOUNT_RK'],
                     'debet_account_rk': row['DEBET_ACCOUNT_RK'],
                     'credit_amount': credit_amount,
                     'debet_amount': debet_amount
                 }

                 connection.execute(text(insert_query), data)
                 rows_processed += 1

             end_time = datetime.now()
             status = "Success"
             log_etl_process(connection, process_name, start_time, end_time, status, rows_processed)

             print(f"Данные успешно загружены в таблицу FT_POSTING_F. Обработано {rows_processed} строк.")

     except Exception as e:
         end_time = datetime.now()
         status = "Failed"
         error_message = str(e)
         log_etl_process(connection, process_name, start_time, end_time, status, 0, error_message)
         print(f"Ошибка при обработке данных для FT_POSTING_F: {e}")



def load_md_account_d(connection, df):
     try:
         df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='%Y-%m-%d').dt.date
         df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d').dt.date
         print("Данные успешно преобразованы для MD_ACCOUNT_D.")
     except Exception as e:
         print(f"Ошибка при преобразовании данных для MD_ACCOUNT_D: {e}")
         return

     update_query = """
     UPDATE ds.MD_ACCOUNT_D
     SET DATA_ACTUAL_END_DATE = :data_actual_end_date,
         ACCOUNT_NUMBER = :account_number,
         CHAR_TYPE = :char_type,
         CURRENCY_RK = :currency_rk,
         CURRENCY_CODE = :currency_code
     WHERE DATA_ACTUAL_DATE = :data_actual_date AND ACCOUNT_RK = :account_rk;
     """

     insert_query = """
     INSERT INTO ds.MD_ACCOUNT_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, ACCOUNT_RK, ACCOUNT_NUMBER, CHAR_TYPE, CURRENCY_RK, CURRENCY_CODE)
     VALUES (:data_actual_date, :data_actual_end_date, :account_rk, :account_number, :char_type, :currency_rk, :currency_code);
     """

     if df.empty:
         print("Ошибка: датафрейм пустой для MD_ACCOUNT_D. Нет данных для загрузки.")
         return

     start_time = datetime.now()
     process_name = "MD_ACCOUNT_D_Load"
     error_message = None

     time.sleep(5)

     try:
         with connection.begin():
             rows_processed = 0
             for _, row in df.iterrows():
                 # Проверка на пустые значения для обязательных полей (PK)
                 if pd.isna(row['ACCOUNT_RK']) or pd.isna(row['DATA_ACTUAL_DATE']):
                     print(f"Пропущена строка с пустыми ключами: ACCOUNT_RK = {row['ACCOUNT_RK']}, DATA_ACTUAL_DATE = {row['DATA_ACTUAL_DATE']}")
                     continue

                 data = {
                     'data_actual_date': row['DATA_ACTUAL_DATE'],
                     'data_actual_end_date': row['DATA_ACTUAL_END_DATE'],
                     'account_rk': row['ACCOUNT_RK'],
                     'account_number': row['ACCOUNT_NUMBER'],
                     'char_type': row['CHAR_TYPE'],
                     'currency_rk': row['CURRENCY_RK'],
                     'currency_code': row['CURRENCY_CODE']
                 }

                 select_query = """
                 SELECT 1 FROM ds.MD_ACCOUNT_D
                 WHERE DATA_ACTUAL_DATE = :data_actual_date AND ACCOUNT_RK = :account_rk;
                 """
                 result = connection.execute(text(select_query), data).fetchone()

                 if result:
                     connection.execute(text(update_query), data)
                 else:
                     connection.execute(text(insert_query), data)

                 rows_processed += 1

             end_time = datetime.now()
             status = "Success"
             log_etl_process(connection, process_name, start_time, end_time, status, rows_processed)

             print(f"Данные успешно загружены или обновлены в таблицу MD_ACCOUNT_D. Обработано {rows_processed} строк.")

     except Exception as e:
         end_time = datetime.now()
         status = "Failed"
         error_message = str(e)
         log_etl_process(connection, process_name, start_time, end_time, status, 0, error_message)
         print(f"Ошибка при обработке данных для MD_ACCOUNT_D: {e}")


def load_md_currency_d(connection, df):
     try:
         df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='%Y-%m-%d').dt.date
         df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d').dt.date
         print("Данные успешно преобразованы для MD_CURRENCY_D.")
     except Exception as e:
         print(f"Ошибка при преобразовании данных для MD_CURRENCY_D: {e}")
         return

     update_query = """
     UPDATE ds.MD_CURRENCY_D
     SET DATA_ACTUAL_END_DATE = :data_actual_end_date,
         CURRENCY_CODE = :currency_code,
         CODE_ISO_CHAR = :code_iso_char
     WHERE CURRENCY_RK = :currency_rk AND DATA_ACTUAL_DATE = :data_actual_date;
     """

     insert_query = """
     INSERT INTO ds.MD_CURRENCY_D (CURRENCY_RK, DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_CODE, CODE_ISO_CHAR)
     VALUES (:currency_rk, :data_actual_date, :data_actual_end_date, :currency_code, :code_iso_char);
     """

     if df.empty:
         print("Ошибка: датафрейм пустой для MD_CURRENCY_D. Нет данных для загрузки.")
         return

     start_time = datetime.now()
     process_name = "MD_CURRENCY_D_Load"

     try:
         with connection.begin():
             rows_processed = 0
             for _, row in df.iterrows():
                 # Проверка на пустые значения для обязательных полей (PK)
                 if pd.isna(row['CURRENCY_RK']) or pd.isna(row['DATA_ACTUAL_DATE']):
                     print(f"Пропущена строка с пустыми ключами: CURRENCY_RK = {row['CURRENCY_RK']}, DATA_ACTUAL_DATE = {row['DATA_ACTUAL_DATE']}")
                     continue

                 currency_code = row['CURRENCY_CODE']
                 code_iso_char = row['CODE_ISO_CHAR']


                 if pd.isna(code_iso_char) or ('�' in str(code_iso_char) or not str(code_iso_char).isprintable()):
                     code_iso_char = None


                 if pd.isna(currency_code) or currency_code == "":
                     currency_code = None
                 elif isinstance(currency_code, str):
                     currency_code = currency_code.zfill(3)[:3]
                 else:
                     currency_code = None

                 data = {
                     'currency_rk': row['CURRENCY_RK'],
                     'data_actual_date': row['DATA_ACTUAL_DATE'],
                     'data_actual_end_date': row['DATA_ACTUAL_END_DATE'],
                     'currency_code': currency_code,
                     'code_iso_char': code_iso_char
                 }


                 select_query = """
                 SELECT 1 FROM ds.MD_CURRENCY_D
                 WHERE CURRENCY_RK = :currency_rk AND DATA_ACTUAL_DATE = :data_actual_date;
                 """
                 result = connection.execute(text(select_query), data).fetchone()

                 if result:
                     connection.execute(text(update_query), data)
                 else:
                     connection.execute(text(insert_query), data)

                 rows_processed += 1

             end_time = datetime.now()
             status = "Success"
             log_etl_process(connection, process_name, start_time, end_time, status, rows_processed)

             print(f"Данные успешно загружены или обновлены в таблицу MD_CURRENCY_D. Обработано {rows_processed} строк.")

     except Exception as e:
         end_time = datetime.now()
         status = "Failed"
         error_message = str(e)
         log_etl_process(connection, process_name, start_time, end_time, status, 0, error_message)
         print(f"Ошибка при обработке данных для MD_CURRENCY_D: {e}")



def load_md_exchange_rate_d(connection, df):
     try:
         df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='%Y-%m-%d').dt.date
         df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d',
                                                     errors='coerce').dt.date
         print("Данные успешно преобразованы для MD_EXCHANGE_RATE_D.")
     except Exception as e:
         print(f"Ошибка при преобразовании данных для MD_EXCHANGE_RATE_D: {e}")
         return


     update_query = """
     UPDATE ds.MD_EXCHANGE_RATE_D
     SET DATA_ACTUAL_END_DATE = :data_actual_end_date,
         REDUCED_COURCE = :reduced_cource,
         CODE_ISO_NUM = :code_iso_num
     WHERE CURRENCY_RK = :currency_rk AND DATA_ACTUAL_DATE = :data_actual_date;
     """

     insert_query = """
     INSERT INTO ds.MD_EXCHANGE_RATE_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_RK, REDUCED_COURCE, CODE_ISO_NUM)
     VALUES (:data_actual_date, :data_actual_end_date, :currency_rk, :reduced_cource, :code_iso_num);
     """

     if df.empty:
         print("Ошибка: датафрейм пустой для MD_EXCHANGE_RATE_D. Нет данных для загрузки.")
         return

     start_time = datetime.now()
     process_name = "MD_EXCHANGE_RATE_D_Load"

     try:
         with connection.begin():
             rows_processed = 0
             for _, row in df.iterrows():
                 # Проверка на пустые значения для первичных ключей (PK)
                 if pd.isna(row['CURRENCY_RK']) or pd.isna(row['DATA_ACTUAL_DATE']):
                     print(f"Пропущена строка с пустыми ключами: CURRENCY_RK = {row['CURRENCY_RK']}, DATA_ACTUAL_DATE = {row['DATA_ACTUAL_DATE']}")
                     continue

                 reduced_cource = row['REDUCED_COURCE'] if pd.notna(row['REDUCED_COURCE']) else None
                 code_iso_num = row['CODE_ISO_NUM'] if pd.notna(row.get('CODE_ISO_NUM', None)) else None

                 if code_iso_num is not None:
                     # Преобразуем код ISO в строку, обрезая или добавляя нули
                     if isinstance(code_iso_num, float):
                         code_iso_num = str(int(code_iso_num))
                     elif isinstance(code_iso_num, int):
                         code_iso_num = str(code_iso_num)

                     if len(code_iso_num) > 3:
                         code_iso_num = code_iso_num[:3]


                 data_actual_end_date = row['DATA_ACTUAL_END_DATE'] if pd.notna(row['DATA_ACTUAL_END_DATE']) else None

                 data = {
                     'data_actual_date': row['DATA_ACTUAL_DATE'],
                     'data_actual_end_date': data_actual_end_date,
                     'currency_rk': row['CURRENCY_RK'],
                     'reduced_cource': reduced_cource,
                     'code_iso_num': code_iso_num
                 }


                 select_query = """
                 SELECT 1 FROM ds.MD_EXCHANGE_RATE_D
                 WHERE CURRENCY_RK = :currency_rk AND DATA_ACTUAL_DATE = :data_actual_date;
                 """
                 result = connection.execute(text(select_query), data).fetchone()


                 if result:
                     connection.execute(text(update_query), data)
                 else:
                     connection.execute(text(insert_query), data)

                 rows_processed += 1

             end_time = datetime.now()
             status = "Success"
             log_etl_process(connection, process_name, start_time, end_time, status, rows_processed)

             print(
                 f"Данные успешно загружены или обновлены в таблицу MD_EXCHANGE_RATE_D. Обработано {rows_processed} строк.")

     except Exception as e:
         end_time = datetime.now()
         status = "Failed"
         error_message = str(e)
         log_etl_process(connection, process_name, start_time, end_time, status, 0, error_message)
         print(f"Ошибка при обработке данных для MD_EXCHANGE_RATE_D: {e}")



def load_md_ledger_account_s(connection, df):
     try:
         df['START_DATE'] = pd.to_datetime(df['START_DATE'], format='%Y-%m-%d').dt.date
         df['END_DATE'] = pd.to_datetime(df['END_DATE'], format='%Y-%m-%d', errors='coerce').dt.date
         print("Данные успешно преобразованы для MD_LEDGER_ACCOUNT_S.")
     except Exception as e:
         print(f"Ошибка при преобразовании данных для MD_LEDGER_ACCOUNT_S: {e}")
         return

     update_query = """
     UPDATE ds.MD_LEDGER_ACCOUNT_S
     SET CHAPTER = :chapter,
         CHAPTER_NAME = :chapter_name,
         SECTION_NUMBER = :section_number,
         SECTION_NAME = :section_name,
         SUBSECTION_NAME = :subsection_name,
         LEDGER1_ACCOUNT = :ledger1_account,
         LEDGER1_ACCOUNT_NAME = :ledger1_account_name,
         LEDGER_ACCOUNT_NAME = :ledger_account_name,
         CHARACTERISTIC = :characteristic,
         IS_RESIDENT = :is_resident,
         IS_RESERVE = :is_reserve,
         IS_RESERVED = :is_reserved,
         IS_LOAN = :is_loan,
         IS_RESERVED_ASSETS = :is_reserved_assets,
         IS_OVERDUE = :is_overdue,
         IS_INTEREST = :is_interest,
         PAIR_ACCOUNT = :pair_account,
         START_DATE = :start_date,
         END_DATE = :end_date,
         IS_RUB_ONLY = :is_rub_only,
         MIN_TERM = :min_term,
         MIN_TERM_MEASURE = :min_term_measure,
         MAX_TERM = :max_term,
         MAX_TERM_MEASURE = :max_term_measure,
         LEDGER_ACC_FULL_NAME_TRANSLIT = :ledger_acc_full_name_translit,
         IS_REVALUATION = :is_revaluation,
         IS_CORRECT = :is_correct
     WHERE LEDGER_ACCOUNT = :ledger_account AND START_DATE = :start_date;
     """

     insert_query = """
     INSERT INTO ds.MD_LEDGER_ACCOUNT_S (CHAPTER, CHAPTER_NAME, SECTION_NUMBER, SECTION_NAME, SUBSECTION_NAME,
     LEDGER1_ACCOUNT, LEDGER1_ACCOUNT_NAME, LEDGER_ACCOUNT, LEDGER_ACCOUNT_NAME, CHARACTERISTIC, IS_RESIDENT,
     IS_RESERVE, IS_RESERVED, IS_LOAN, IS_RESERVED_ASSETS, IS_OVERDUE, IS_INTEREST, PAIR_ACCOUNT, START_DATE,
     END_DATE, IS_RUB_ONLY, MIN_TERM, MIN_TERM_MEASURE, MAX_TERM, MAX_TERM_MEASURE, LEDGER_ACC_FULL_NAME_TRANSLIT,
     IS_REVALUATION, IS_CORRECT)
     VALUES (:chapter, :chapter_name, :section_number, :section_name, :subsection_name, :ledger1_account,
     :ledger1_account_name, :ledger_account, :ledger_account_name, :characteristic, :is_resident, :is_reserve,
     :is_reserved, :is_loan, :is_reserved_assets, :is_overdue, :is_interest, :pair_account, :start_date, :end_date,
     :is_rub_only, :min_term, :min_term_measure, :max_term, :max_term_measure, :ledger_acc_full_name_translit,
     :is_revaluation, :is_correct);
     """

     if df.empty:
         print("Ошибка: датафрейм пустой для MD_LEDGER_ACCOUNT_S. Нет данных для загрузки.")
         return

     start_time = datetime.now()
     process_name = "MD_LEDGER_ACCOUNT_S_Load"

     try:
         with connection.begin():
             rows_processed = 0
             for _, row in df.iterrows():
                 # Проверка на пустые значения для первичных ключей (PK)
                 if pd.isna(row['LEDGER_ACCOUNT']) or pd.isna(row['START_DATE']):
                     print(f"Пропущена строка с пустыми ключами: LEDGER_ACCOUNT = {row['LEDGER_ACCOUNT']}, START_DATE = {row['START_DATE']}")
                     continue

                 chapter = row['CHAPTER'] if pd.notna(row['CHAPTER']) else None
                 chapter_name = row['CHAPTER_NAME'] if pd.notna(row['CHAPTER_NAME']) else None
                 section_number = row['SECTION_NUMBER'] if pd.notna(row['SECTION_NUMBER']) else None
                 section_name = row['SECTION_NAME'] if pd.notna(row['SECTION_NAME']) else None
                 subsection_name = row['SUBSECTION_NAME'] if pd.notna(row['SUBSECTION_NAME']) else None
                 ledger1_account = row['LEDGER1_ACCOUNT'] if pd.notna(row['LEDGER1_ACCOUNT']) else None
                 ledger1_account_name = row['LEDGER1_ACCOUNT_NAME'] if pd.notna(row['LEDGER1_ACCOUNT_NAME']) else None
                 ledger_account_name = row['LEDGER_ACCOUNT_NAME'] if pd.notna(row['LEDGER_ACCOUNT_NAME']) else None
                 characteristic = row['CHARACTERISTIC'] if pd.notna(row['CHARACTERISTIC']) else None


                 is_resident = None
                 is_reserve = None
                 is_reserved = None
                 is_loan = None
                 is_reserved_assets = None
                 is_overdue = None
                 is_interest = None
                 pair_account = None
                 end_date = row['END_DATE'] if pd.notna(row['END_DATE']) else None
                 is_rub_only = None
                 min_term = None
                 min_term_measure = None
                 max_term = None
                 max_term_measure = None
                 ledger_acc_full_name_translit = None
                 is_revaluation = None
                 is_correct = None


                 data = {
                     'chapter': chapter,
                     'chapter_name': chapter_name,
                     'section_number': section_number,
                     'section_name': section_name,
                     'subsection_name': subsection_name,
                     'ledger1_account': ledger1_account,
                     'ledger1_account_name': ledger1_account_name,
                     'ledger_account': row['LEDGER_ACCOUNT'],
                     'ledger_account_name': ledger_account_name,
                     'characteristic': characteristic,
                     'start_date': row['START_DATE'],
                     'end_date': end_date,
                     'is_resident': is_resident,
                     'is_reserve': is_reserve,
                     'is_reserved': is_reserved,
                     'is_loan': is_loan,
                     'is_reserved_assets': is_reserved_assets,
                     'is_overdue': is_overdue,
                     'is_interest': is_interest,
                     'pair_account': pair_account,
                     'is_rub_only': is_rub_only,
                     'min_term': min_term,
                     'min_term_measure': min_term_measure,
                     'max_term': max_term,
                     'max_term_measure': max_term_measure,
                     'ledger_acc_full_name_translit': ledger_acc_full_name_translit,
                     'is_revaluation': is_revaluation,
                     'is_correct': is_correct
                 }


                 select_query = """
                    SELECT 1 FROM ds.MD_LEDGER_ACCOUNT_S
                    WHERE LEDGER_ACCOUNT = :ledger_account AND START_DATE = :start_date;
                    """
                 result = connection.execute(text(select_query), data).fetchone()

                 if result:
                     connection.execute(text(update_query), data)
                 else:
                     connection.execute(text(insert_query), data)

                 rows_processed += 1

             end_time = datetime.now()
             status = "Success"
             log_etl_process(connection, process_name, start_time, end_time, status, rows_processed)

             print(f"Данные успешно загружены или обновлены в таблицу MD_LEDGER_ACCOUNT_S. Обработано {rows_processed} строк.")

     except Exception as e:
         end_time = datetime.now()
         status = "Failed"
         error_message = str(e)
         log_etl_process(connection, process_name, start_time, end_time, status, 0, error_message)
         print(f"Ошибка при обработке данных для MD_LEDGER_ACCOUNT_S: {e}")


def menu():
     host = "localhost"
     port = "5432"
     dbname = "postgres"
     user = "postgres"

     engine = create_engine(f'postgresql+psycopg2://{user}@{host}:{port}/{dbname}')

     with engine.connect() as connection:
         while True:
             print("\nВыберите действие:")
             print("1. Полная загрузка")
             print("2. Загрузка только FT_BALANCE_F")
             print("3. Загрузка только FT_POSTING_F")
             print("4. Загрузка только MD_ACCOUNT_D")
             print("5. Загрузка только MD_CURRENCY_D")
             print("6. Загрузка только MD_EXCHANGE_RATE_D")
             print("7. Загрузка только MD_LEDGER_ACCOUNT_S")
             print("8. Выход")

             choice = input("Введите номер действия: ")

             if choice == '1':
                 print("Запуск всех потоков...")
                 df_ft_balance_f = pd.read_csv(f"{BASE_PATH}/ft_balance_f.csv", delimiter=";")
                 df_ft_posting_f = pd.read_csv(f"{BASE_PATH}/ft_posting_f.csv", delimiter=";")
                 df_md_account_d = pd.read_csv(f"{BASE_PATH}/md_account_d.csv", delimiter=";")
                 df_md_currency_d = pd.read_csv(f"{BASE_PATH}/md_currency_d.csv", delimiter=";",
                                                encoding="windows-1252")
                 df_md_exchange_rate_d = pd.read_csv(f"{BASE_PATH}/md_exchange_rate_d.csv", delimiter=";",
                                                     encoding="windows-1252")
                 df_md_ledger_account_s = pd.read_csv(f"{BASE_PATH}/md_ledger_account_s.csv", delimiter=";")

                 load_ft_balance_f(connection, df_ft_balance_f)
                 load_ft_posting_f(connection, df_ft_posting_f)
                 load_md_account_d(connection, df_md_account_d)
                 load_md_currency_d(connection, df_md_currency_d)
                 load_md_exchange_rate_d(connection, df_md_exchange_rate_d)
                 load_md_ledger_account_s(connection, df_md_ledger_account_s)

             elif choice == '2':
                 print("Запуск FT_BALANCE_F потока...")
                 df_ft_balance_f = pd.read_csv(f"{BASE_PATH}/ft_balance_f.csv", delimiter=";")
                 load_ft_balance_f(connection, df_ft_balance_f)

             elif choice == '3':
                 print("Запуск FT_POSTING_F потока...")
                 df_ft_posting_f = pd.read_csv(f"{BASE_PATH}/ft_posting_f.csv", delimiter=";")
                 load_ft_posting_f(connection, df_ft_posting_f)

             elif choice == '4':

                 print("Запуск MD_ACCOUNT_D потока...")
                 df_md_account_d = pd.read_csv(f"{BASE_PATH}/md_account_d.csv", delimiter=";")
                 load_md_account_d(connection, df_md_account_d)

             elif choice == '5':

                 print("Запуск MD_CURRENCY_D потока...")
                 df_md_currency_d = pd.read_csv(f"{BASE_PATH}/md_currency_d.csv", delimiter=";",
                                                encoding="windows-1252")
                 load_md_currency_d(connection, df_md_currency_d)

             elif choice == '6':
                 print("Запуск MD_EXCHANGE_RATE_D потока...")
                 df_md_exchange_rate_d = pd.read_csv(f"{BASE_PATH}/md_exchange_rate_d.csv", delimiter=";",
                                                encoding="windows-1252")
                 load_md_exchange_rate_d(connection, df_md_exchange_rate_d)

             elif choice == '7':
                 print("Запуск MD_LEDGER_ACCOUNT_S потока...")
                 df_md_ledger_account_s = pd.read_csv(f"{BASE_PATH}/md_ledger_account_s.csv", delimiter=";")
                 load_md_ledger_account_s(connection, df_md_ledger_account_s)

             elif choice == '8':
                 print("Выход...")
                 break

             else:
                 print("Неверный ввод, попробуйте снова.")

if __name__ == "__main__":
    menu()

