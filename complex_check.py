from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def check_db_and_write():
    # 使用你之前在 UI 建立的 Connection ID
    hook = PostgresHook(postgres_conn_id='my_pg_db')
    
    # 打印一条日志，并在数据库里建个表
    sql = "CREATE TABLE IF NOT EXISTS flow_log (log_time timestamp, status text);"
    hook.run(sql)
    hook.run("INSERT INTO flow_log VALUES (now(), 'Success');")
    print("✅ 数据库写入成功！环境全链路已打通。")

with DAG(
    'final_boss_check',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_db = PythonOperator(
        task_id='check_database_connection',
        python_callable=check_db_and_write
    )