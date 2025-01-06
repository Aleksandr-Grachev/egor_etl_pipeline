from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

from datetime import datetime
import pandas as pd
import chardet

PATH = Variable.get("my_path")

conf.set("core", "template_searchpath", PATH)

def insert_data(table_name):
    file_path = PATH + f"{table_name}.csv"
    
    with open(file_path, 'rb') as file:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
    

    df = pd.read_csv(file_path, delimiter=";", encoding=encoding)

    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()


    df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False)

default_args= {
    "owner" : "egor",
    "start_date" :  datetime(2024,12,23),
    "retries" : 2
}

with DAG(
    "etl_pypeline_sql",
    default_args= default_args,
    description = "Загрузка данных",
    catchup = False,
    template_searchpath = [PATH],
    # schedule = "0 0 * * *"
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    sql_trunc_stage = SQLExecuteQueryOperator(
        task_id="sql_trunc_stage",
        conn_id="postgres-db",
        sql = "sql/sql_trunc_stage.sql"
    )

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_account_d"}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_currency_d"}
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )

    split = DummyOperator(
        task_id="split"
    )

    sql_create_table = SQLExecuteQueryOperator(
        task_id="sql_create_table",
        conn_id="postgres-db",
        sql = "sql/sql_create_table.sql"
    )

    sql_ft_balance_f = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f",
        conn_id="postgres-db",
        sql = "sql/ft_balance_f.sql"
    )

    sql_ft_posting_f = SQLExecuteQueryOperator(
        task_id="sql_ft_posting_f",
        conn_id="postgres-db",
        sql = "sql/ft_posting_f.sql"
    )

    sql_md_account_d = SQLExecuteQueryOperator(
        task_id="sql_md_account_d",
        conn_id="postgres-db",
        sql = "sql/md_account_d.sql"
    )

    sql_md_currency_d = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d",
        conn_id="postgres-db",
        sql = "sql/md_currency_d.sql"
    )

    sql_md_exchange_rate_d = SQLExecuteQueryOperator(
        task_id="sql_md_exchange_rate_d",
        conn_id="postgres-db",
        sql = "sql/md_exchange_rate_d.sql"
    )

    sql_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s",
        conn_id="postgres-db",
        sql = "sql/md_ledger_account_s.sql"
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >> sql_trunc_stage
        >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s]
        >> split
        >> sql_create_table
        >>[sql_ft_balance_f, sql_ft_posting_f, sql_md_account_d, sql_md_currency_d, sql_md_exchange_rate_d, sql_md_ledger_account_s]
        >> end
    )