from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.configuration import conf
#from sqlalchemy.sql import text
from sqlalchemy import text
from datetime import datetime, timedelta
import pandas as pd
import time

PATH = Variable.get("my_path")

conf.set("core", "template_searchpath", PATH)

def log_etl_process(connection, process_name, start_time, end_time, status, rows_processed, error_message=None):
    insert_log_query = """
<<<<<<<< HEAD:src/dags/task1/2/etl_pipeline.py
    INSERT INTO LOGS.ETL_LOG (process_name, start_time, end_time, status, rows_processed, ERROR_MESSAGE)
========
    INSERT INTO logs.ETL_LOG (process_name, start_time, end_time, status, rows_processed, ERROR_MESSAGE)
>>>>>>>> main:1TASK/1_task/Task_ETL/2/etl_pipeline.py
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

def load_ft_balance_f():
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df = pd.read_csv(f"{PATH}/csv/task1/ft_balance_f.csv", delimiter=";", encoding="utf-8")

    
    df['ON_DATE'] = pd.to_datetime(df['ON_DATE'], format='%d.%m.%Y').dt.date

    # убираем строки с пустыми значениями для PK
    df = df.dropna(subset=['ON_DATE', 'ACCOUNT_RK'])

    upsert_query = """
<<<<<<<< HEAD:src/dags/task1/2/etl_pipeline.py
    INSERT INTO DS.FT_BALANCE_F (ON_DATE, ACCOUNT_RK, CURRENCY_RK, BALANCE_OUT)
========
    INSERT INTO ds.FT_BALANCE_F (ON_DATE, ACCOUNT_RK, CURRENCY_RK, BALANCE_OUT)
>>>>>>>> main:1TASK/1_task/Task_ETL/2/etl_pipeline.py
    VALUES (:on_date, :account_rk, :currency_rk, :balance_out)
    ON CONFLICT (ON_DATE, ACCOUNT_RK) 
    DO UPDATE SET 
        balance_out = EXCLUDED.balance_out, 
        currency_rk = EXCLUDED.currency_rk;
    """

    start_time = datetime.now()
    process_name = "FT_BALANCE_F_Load"
    rows_processed = 0
    skipped_rows = 0

    data_to_process = []
    
    for _, row in df.iterrows():
        data = {
            'on_date': row['ON_DATE'],
            'account_rk': row['ACCOUNT_RK'],
            'currency_rk': row['CURRENCY_RK'] if pd.notna(row['CURRENCY_RK']) else None,
            'balance_out': row['BALANCE_OUT'] if pd.notna(row['BALANCE_OUT']) else None
        }
        data_to_process.append(data)

    with engine.connect() as connection:
        try:
            
            connection.execute(text(upsert_query), data_to_process)

            time.sleep(5)

            rows_processed = len(data_to_process)

            # Логи
            log_etl_process(
                connection, process_name, start_time, datetime.now(), "Success", rows_processed,
                f"Rows skipped: {skipped_rows}"
            )

        except Exception as e:
            log_etl_process(connection, process_name, start_time, datetime.now(), "Failed", 0, str(e))
            raise



#-----------------------


def load_ft_posting_f():
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df = pd.read_csv(f"{PATH}/csv/task1/ft_posting_f.csv", delimiter=";", encoding="utf-8")
    
    df['OPER_DATE'] = pd.to_datetime(df['OPER_DATE'], format='%d-%m-%Y').dt.date
    df = df.where(pd.notnull(df), None)

<<<<<<<< HEAD:src/dags/task1/2/etl_pipeline.py
    clear_query = 'TRUNCATE TABLE DS.FT_POSTING_F;'

    insert_query = """
    INSERT INTO DS.FT_POSTING_F (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK, CREDIT_AMOUNT, DEBET_AMOUNT)
========
    clear_query = 'TRUNCATE TABLE ds.FT_POSTING_F;'

    insert_query = """
    INSERT INTO ds.FT_POSTING_F (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK, CREDIT_AMOUNT, DEBET_AMOUNT)
>>>>>>>> main:1TASK/1_task/Task_ETL/2/etl_pipeline.py
    VALUES (:oper_date, :credit_account_rk, :debet_account_rk, :credit_amount, :debet_amount);
    """

    start_time = datetime.now()
    process_name = "FT_POSTING_F_Load"
    rows_processed = 0
    skipped_rows = 0

    with engine.connect() as connection:
        try:
            connection.execute(text(clear_query))
            
            # убираем строки с пустыми значениями для PK
            df_cleaned = df.dropna(subset=['OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK'])
            
            data_to_insert = []
            for _, row in df_cleaned.iterrows():
                data = {
                    'oper_date': row['OPER_DATE'],
                    'credit_account_rk': row['CREDIT_ACCOUNT_RK'],
                    'debet_account_rk': row['DEBET_ACCOUNT_RK'],
                    'credit_amount': row['CREDIT_AMOUNT'] if pd.notna(row['CREDIT_AMOUNT']) else None,
                    'debet_amount': row['DEBET_AMOUNT'] if pd.notna(row['DEBET_AMOUNT']) else None
                }
                data_to_insert.append(data)

            connection.execute(text(insert_query), data_to_insert)

            time.sleep(5)

            rows_processed = len(data_to_insert)
            skipped_rows = len(df) - rows_processed

            # Логи
            log_etl_process(
                connection, process_name, start_time, datetime.now(), "Success", rows_processed,
                f"Rows skipped: {skipped_rows}"
            )
        except Exception as e:
            log_etl_process(connection, process_name, start_time, datetime.now(), "Failed", 0, str(e))
            raise

#-----------------------

def load_md_account_d():
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df = pd.read_csv(f"{PATH}/csv/task1/md_account_d.csv", delimiter=";", encoding="utf-8")

    df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='%Y-%m-%d').dt.date
    df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d').dt.date

    # убираем строки с пустыми значениями для PK
    df = df.dropna(subset=['ACCOUNT_RK', 'DATA_ACTUAL_DATE'])  

    upsert_query = """
<<<<<<<< HEAD:src/dags/task1/2/etl_pipeline.py
    INSERT INTO DS.MD_ACCOUNT_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, ACCOUNT_RK, ACCOUNT_NUMBER, CHAR_TYPE, CURRENCY_RK, CURRENCY_CODE)
========
    INSERT INTO ds.MD_ACCOUNT_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, ACCOUNT_RK, ACCOUNT_NUMBER, CHAR_TYPE, CURRENCY_RK, CURRENCY_CODE)
>>>>>>>> main:1TASK/1_task/Task_ETL/2/etl_pipeline.py
    VALUES (:data_actual_date, :data_actual_end_date, :account_rk, :account_number, :char_type, :currency_rk, :currency_code)
    ON CONFLICT (DATA_ACTUAL_DATE, ACCOUNT_RK) 
    DO UPDATE SET 
        DATA_ACTUAL_END_DATE = EXCLUDED.DATA_ACTUAL_END_DATE,
        ACCOUNT_NUMBER = EXCLUDED.ACCOUNT_NUMBER,
        CHAR_TYPE = EXCLUDED.CHAR_TYPE,
        CURRENCY_RK = EXCLUDED.CURRENCY_RK,
        CURRENCY_CODE = EXCLUDED.CURRENCY_CODE;
    """

    start_time = datetime.now()
    process_name = "MD_ACCOUNT_Load"
    rows_processed = 0
    skipped_rows = 0

    data_to_process = []
    
    for _, row in df.iterrows():
        data = {
            'data_actual_date': row['DATA_ACTUAL_DATE'],
            'data_actual_end_date': row['DATA_ACTUAL_END_DATE'],
            'account_rk': row['ACCOUNT_RK'],
            'account_number': row['ACCOUNT_NUMBER'],
            'char_type': row['CHAR_TYPE'],
            'currency_rk': row['CURRENCY_RK'],
            'currency_code': row['CURRENCY_CODE']
        }
        data_to_process.append(data)

    with engine.connect() as connection:
        try:
            connection.execute(text(upsert_query), data_to_process)

            time.sleep(5)

            rows_processed = len(data_to_process)

            # Логи
            log_etl_process(
                connection, process_name, start_time, datetime.now(), "Success", rows_processed,
                f"Rows skipped: {skipped_rows}"
            )

        except Exception as e:
            log_etl_process(connection, process_name, start_time, datetime.now(), "Failed", 0, str(e))
            raise



#-----------------------


def load_md_currency_d():
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df = pd.read_csv(
        f"{PATH}/csv/task1/md_currency_d.csv",
        delimiter=";",
        encoding="windows-1252",
        dtype={"CURRENCY_CODE": str}  # сохранение ведущих нулей
    )

    df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='%Y-%m-%d').dt.date
    df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d').dt.date
    df['CURRENCY_CODE'] = df['CURRENCY_CODE'].astype(str).str[:3]

    # убираем строки с пустыми значениями для PK
    df = df.dropna(subset=['CURRENCY_RK', 'DATA_ACTUAL_DATE'])  

    upsert_query = """
<<<<<<<< HEAD:src/dags/task1/2/etl_pipeline.py
    INSERT INTO DS.MD_CURRENCY_D (CURRENCY_RK, DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_CODE, CODE_ISO_CHAR)
========
    INSERT INTO ds.MD_CURRENCY_D (CURRENCY_RK, DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_CODE, CODE_ISO_CHAR)
>>>>>>>> main:1TASK/1_task/Task_ETL/2/etl_pipeline.py
    VALUES (:currency_rk, :data_actual_date, :data_actual_end_date, :currency_code, :code_iso_char)
    ON CONFLICT (CURRENCY_RK, DATA_ACTUAL_DATE)
    DO UPDATE SET
        DATA_ACTUAL_END_DATE = EXCLUDED.DATA_ACTUAL_END_DATE,
        CURRENCY_CODE = EXCLUDED.CURRENCY_CODE,
        CODE_ISO_CHAR = EXCLUDED.CODE_ISO_CHAR;
    """

    start_time = datetime.now()
    process_name = "MD_CURRENCY_Load"
    rows_processed = 0
    skipped_rows = 0

    data_to_process = []

    for _, row in df.iterrows():
        data = {
            'currency_rk': row['CURRENCY_RK'],
            'data_actual_date': row['DATA_ACTUAL_DATE'],
            'data_actual_end_date': row['DATA_ACTUAL_END_DATE'] if pd.notna(row['DATA_ACTUAL_END_DATE']) else None,
            'currency_code': row['CURRENCY_CODE'] if pd.notna(row['CURRENCY_CODE']) else None,
            'code_iso_char': row['CODE_ISO_CHAR'] if pd.notna(row['CODE_ISO_CHAR']) else None
        }
        data_to_process.append(data)

    with engine.connect() as connection:
        try:
            connection.execute(text(upsert_query), data_to_process)

            time.sleep(5)

            rows_processed = len(data_to_process)

            # Логи
            log_etl_process(
                connection, process_name, start_time, datetime.now(), "Success", rows_processed,
                f"Rows skipped: {skipped_rows}"
            )

        except Exception as e:
            log_etl_process(connection, process_name, start_time, datetime.now(), "Failed", 0, str(e))
            raise




#-----------------------


def load_md_exchange_rate_d():
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()
    
    df = pd.read_csv(f"{PATH}/csv/task1/md_exchange_rate_d.csv", delimiter=";", encoding="utf-8")
    
    df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='%Y-%m-%d').dt.date
    df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d', errors='coerce').dt.date
    df['CODE_ISO_NUM'] = df['CODE_ISO_NUM'].apply(lambda x: str(int(x))[:3] if pd.notna(x) else None)

    # убираем строки с пустыми значениями для PK
    df = df.dropna(subset=['CURRENCY_RK', 'DATA_ACTUAL_DATE'])

    if df.empty:
        print("Ошибка: датафрейм пустой для MD_EXCHANGE_RATE_D. Нет данных для загрузки.")
        return

    upsert_query = """
<<<<<<<< HEAD:src/dags/task1/2/etl_pipeline.py
    INSERT INTO DS.MD_EXCHANGE_RATE_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_RK, REDUCED_COURCE, CODE_ISO_NUM)
========
    INSERT INTO ds.MD_EXCHANGE_RATE_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_RK, REDUCED_COURCE, CODE_ISO_NUM)
>>>>>>>> main:1TASK/1_task/Task_ETL/2/etl_pipeline.py
    VALUES (:data_actual_date, :data_actual_end_date, :currency_rk, :reduced_cource, :code_iso_num)
    ON CONFLICT (CURRENCY_RK, DATA_ACTUAL_DATE)
    DO UPDATE SET
        DATA_ACTUAL_END_DATE = EXCLUDED.DATA_ACTUAL_END_DATE,
        REDUCED_COURCE = EXCLUDED.REDUCED_COURCE,
        CODE_ISO_NUM = EXCLUDED.CODE_ISO_NUM;
    """

    start_time = datetime.now()
    process_name = "MD_EXCHANGE_RATE_D_Load"
    rows_processed = 0
    skipped_rows = 0


    data_to_process = []

    for _, row in df.iterrows():
        data = {
            'data_actual_date': row['DATA_ACTUAL_DATE'],
            'data_actual_end_date': row['DATA_ACTUAL_END_DATE'] if pd.notna(row['DATA_ACTUAL_END_DATE']) else None,
            'currency_rk': row['CURRENCY_RK'],
            'reduced_cource': row['REDUCED_COURCE'] if pd.notna(row['REDUCED_COURCE']) else None,
            'code_iso_num': row['CODE_ISO_NUM']
        }
        data_to_process.append(data)

    with engine.connect() as connection:
        try:
            connection.execute(text(upsert_query), data_to_process)

            time.sleep(5)

            rows_processed = len(data_to_process)

            # Логи
            log_etl_process(
                connection, process_name, start_time, datetime.now(), "Success", rows_processed,
                f"Rows skipped: {skipped_rows}"
            )

        except Exception as e:
            log_etl_process(connection, process_name, start_time, datetime.now(), "Failed", 0, str(e))
            raise


#-----------------------

def load_md_ledger_account_s():
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()

    df = pd.read_csv(f"{PATH}/csv/task1/md_ledger_account_s.csv", delimiter=";", encoding="utf-8")
    
    df['START_DATE'] = pd.to_datetime(df['START_DATE'], format='%Y-%m-%d').dt.date
    df['END_DATE'] = pd.to_datetime(df['END_DATE'], format='%Y-%m-%d', errors='coerce').dt.date

    # убираем строки с пустыми значениями для PK
    df = df.dropna(subset=['LEDGER_ACCOUNT', 'START_DATE'])

    if df.empty:
        print("Ошибка: датафрейм пустой для MD_LEDGER_ACCOUNT_S. Нет данных для загрузки.")
        return

    upsert_query = """
<<<<<<<< HEAD:src/dags/task1/2/etl_pipeline.py
    INSERT INTO DS.MD_LEDGER_ACCOUNT_S (CHAPTER, CHAPTER_NAME, SECTION_NUMBER, SECTION_NAME, SUBSECTION_NAME,
========
    INSERT INTO ds.MD_LEDGER_ACCOUNT_S (CHAPTER, CHAPTER_NAME, SECTION_NUMBER, SECTION_NAME, SUBSECTION_NAME,
>>>>>>>> main:1TASK/1_task/Task_ETL/2/etl_pipeline.py
    LEDGER1_ACCOUNT, LEDGER1_ACCOUNT_NAME, LEDGER_ACCOUNT, LEDGER_ACCOUNT_NAME, CHARACTERISTIC, IS_RESIDENT,
    IS_RESERVE, IS_RESERVED, IS_LOAN, IS_RESERVED_ASSETS, IS_OVERDUE, IS_INTEREST, PAIR_ACCOUNT, START_DATE,
    END_DATE, IS_RUB_ONLY, MIN_TERM, MIN_TERM_MEASURE, MAX_TERM, MAX_TERM_MEASURE, LEDGER_ACC_FULL_NAME_TRANSLIT,
    IS_REVALUATION, IS_CORRECT)
    VALUES (:chapter, :chapter_name, :section_number, :section_name, :subsection_name, :ledger1_account,
    :ledger1_account_name, :ledger_account, :ledger_account_name, :characteristic, :is_resident, :is_reserve,
    :is_reserved, :is_loan, :is_reserved_assets, :is_overdue, :is_interest, :pair_account, :start_date, :end_date,
    :is_rub_only, :min_term, :min_term_measure, :max_term, :max_term_measure, :ledger_acc_full_name_translit,
    :is_revaluation, :is_correct)
    ON CONFLICT (LEDGER_ACCOUNT, START_DATE)
    DO UPDATE SET
        CHAPTER = EXCLUDED.CHAPTER,
        CHAPTER_NAME = EXCLUDED.CHAPTER_NAME,
        SECTION_NUMBER = EXCLUDED.SECTION_NUMBER,
        SECTION_NAME = EXCLUDED.SECTION_NAME,
        SUBSECTION_NAME = EXCLUDED.SUBSECTION_NAME,
        LEDGER1_ACCOUNT = EXCLUDED.LEDGER1_ACCOUNT,
        LEDGER1_ACCOUNT_NAME = EXCLUDED.LEDGER1_ACCOUNT_NAME,
        LEDGER_ACCOUNT_NAME = EXCLUDED.LEDGER_ACCOUNT_NAME,
        CHARACTERISTIC = EXCLUDED.CHARACTERISTIC,
        IS_RESIDENT = EXCLUDED.IS_RESIDENT,
        IS_RESERVE = EXCLUDED.IS_RESERVE,
        IS_RESERVED = EXCLUDED.IS_RESERVED,
        IS_LOAN = EXCLUDED.IS_LOAN,
        IS_RESERVED_ASSETS = EXCLUDED.IS_RESERVED_ASSETS,
        IS_OVERDUE = EXCLUDED.IS_OVERDUE,
        IS_INTEREST = EXCLUDED.IS_INTEREST,
        PAIR_ACCOUNT = EXCLUDED.PAIR_ACCOUNT,
        IS_RUB_ONLY = EXCLUDED.IS_RUB_ONLY,
        MIN_TERM = EXCLUDED.MIN_TERM,
        MIN_TERM_MEASURE = EXCLUDED.MIN_TERM_MEASURE,
        MAX_TERM = EXCLUDED.MAX_TERM,
        MAX_TERM_MEASURE = EXCLUDED.MAX_TERM_MEASURE,
        LEDGER_ACC_FULL_NAME_TRANSLIT = EXCLUDED.LEDGER_ACC_FULL_NAME_TRANSLIT,
        IS_REVALUATION = EXCLUDED.IS_REVALUATION,
        IS_CORRECT = EXCLUDED.IS_CORRECT;
    """

    start_time = datetime.now()
    process_name = "MD_LEDGER_ACCOUNT_S_Load"
    rows_processed = 0
    skipped_rows = 0

    data_to_process = []

    for _, row in df.iterrows():
        data = {
            'chapter': row.get('CHAPTER'),
            'chapter_name': row.get('CHAPTER_NAME'),
            'section_number': row.get('SECTION_NUMBER'),
            'section_name': row.get('SECTION_NAME'),
            'subsection_name': row.get('SUBSECTION_NAME'),
            'ledger1_account': row.get('LEDGER1_ACCOUNT'),
            'ledger1_account_name': row.get('LEDGER1_ACCOUNT_NAME'),
            'ledger_account': row['LEDGER_ACCOUNT'],
            'ledger_account_name': row.get('LEDGER_ACCOUNT_NAME'),
            'characteristic': row.get('CHARACTERISTIC'),
            'start_date': row['START_DATE'],
            'end_date': row.get('END_DATE'),
            'is_resident': row.get('IS_RESIDENT'),
            'is_reserve': row.get('IS_RESERVE'),
            'is_reserved': row.get('IS_RESERVED'),
            'is_loan': row.get('IS_LOAN'),
            'is_reserved_assets': row.get('IS_RESERVED_ASSETS'),
            'is_overdue': row.get('IS_OVERDUE'),
            'is_interest': row.get('IS_INTEREST'),
            'pair_account': row.get('PAIR_ACCOUNT'),
            'is_rub_only': row.get('IS_RUB_ONLY'),
            'min_term': row.get('MIN_TERM'),
            'min_term_measure': row.get('MIN_TERM_MEASURE'),
            'max_term': row.get('MAX_TERM'),
            'max_term_measure': row.get('MAX_TERM_MEASURE'),
            'ledger_acc_full_name_translit': row.get('LEDGER_ACC_FULL_NAME_TRANSLIT'),
            'is_revaluation': row.get('IS_REVALUATION'),
            'is_correct': row.get('IS_CORRECT')
        }
        data_to_process.append(data)

    with engine.connect() as connection:
        try:
            connection.execute(text(upsert_query), data_to_process)

            time.sleep(5)

            rows_processed = len(data_to_process)

            # Логи
            log_etl_process(
                connection, process_name, start_time, datetime.now(), "Success", rows_processed,
                f"Rows skipped: {skipped_rows}"
            )

        except Exception as e:
            log_etl_process(connection, process_name, start_time, datetime.now(), "Failed", 0, str(e))
            raise



default_args = {
    "owner": "egor",
    "start_date": datetime(2024, 12, 23),
    "retries": 1,
    "retry_delay": timedelta(seconds=7)
}

with DAG(
    "etl_pipeline",
    default_args=default_args,
    description="ETL Data Pipeline",
    catchup=False,
    schedule_interval=None
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    ft_balance_f_task = PythonOperator(
        task_id="load_ft_balance_f",
        python_callable=load_ft_balance_f
    )

    ft_posting_f_task = PythonOperator(
        task_id="load_ft_posting_f",
        python_callable=load_ft_posting_f
    )

    md_account_d_task = PythonOperator(
        task_id="load_md_account_d",
        python_callable=load_md_account_d
    )

    md_currency_d_task = PythonOperator(
        task_id="load_md_currency_d",
        python_callable=load_md_currency_d
    )

    md_exchange_rate_d_task = PythonOperator(
        task_id="load_md_exchange_rate_d",
        python_callable=load_md_exchange_rate_d
    )

    md_ledger_account_s_task = PythonOperator(
        task_id="load_md_ledger_account_s",
        python_callable=load_md_ledger_account_s
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> [ft_balance_f_task, ft_posting_f_task, md_account_d_task, md_currency_d_task, md_exchange_rate_d_task, md_ledger_account_s_task] >> end
