from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime

# 1. 定义数据处理逻辑
def etl_process():
    # --- Step 1: Extract (模拟生成数据) ---
    raw_data = {
        'user_id': [1, 2, 3],
        'order_amount': [100.5, 200.0, 150.75],
        'category': ['Electronics', 'Books', 'Electronics']
    }
    df = pd.DataFrame(raw_data)
    
    # --- Step 2: Transform (用 Pandas 处理数据) ---
    # 模拟：给 Electronics 类产品打 9 折
    df['final_amount'] = df.apply(
        lambda x: x['order_amount'] * 0.9 if x['category'] == 'Electronics' else x['order_amount'], 
        axis=1
    )
    df['processed_at'] = datetime.now()
    
    # --- Step 3: Load (写入 Postgres) ---
    # 使用 Hook，它是 Operator 的底层，更适合在 Python 函数里操作数据库
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 获取 SQLAlchemy 引擎来配合 Pandas 的 to_sql
    engine = pg_hook.get_sqlalchemy_engine()
    
    # 将 DataFrame 写入数据库表 (如果表不存在会自动创建)
    df.to_sql('processed_orders', con=engine, if_exists='replace', index=False)
    print("数据已成功通过 Pandas 写入 Postgres！")

# 2. 定义 DAG
with DAG(
    'pandas_to_postgres_v1',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='run_pandas_etl',
        python_callable=etl_process
    )
