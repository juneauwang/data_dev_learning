from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

# 1. 定义数据处理逻辑
def etl_process():
    # --- Step 1: Extract (模拟生成数据) ---
    raw_data = {
        'user_id': [1, 2, 3, 4],
        'order_amount': [100.5, 200.0, 150.75, -50.5],
        'category': ['Electronics', 'Books', 'Electronics', 'Books']
    }
    df = pd.DataFrame(raw_data)
    
    # --- Step 2: Transform (用 Pandas 处理数据) ---
    # 模拟：给 Electronics 类产品打 9 折
    df['final_amount'] = df.apply(
        lambda x: x['order_amount'] * 0.9 if x['category'] == 'Electronics' else x['order_amount'], 
        axis=1
    )
    df['processed_at'] = datetime.now()

# --- Step 3: Data Quality Check (新加入的拦截器) ---
    # 检查是否有负数金额
    if (df['final_amount'] < 0).any():
        # 记录下错误详情
        invalid_count = (df['final_amount'] < 0).sum()
        error_msg = f"❌ 数据校验失败：发现 {invalid_count} 条负数订单，拒绝入库！"
        print(error_msg)
        # 抛出异常，强制让 Airflow 任务失败
        raise ValueError(error_msg)    
    # --- Step 4: Load (写入 Postgres) ---
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 【核心修改点】：手动从 Hook 获取 URI 并创建 engine
    # 这会绕过 get_sqlalchemy_engine() 中那个会导致 __extra__ 报错的逻辑
    connection_uri = pg_hook.get_uri()
    print (connection_uri) 
    # 如果 URI 里包含了 __extra__，我们手动把它剔除（这是最稳妥的过滤）
    if "__extra__" in connection_uri:
        connection_uri = connection_uri.split("?")[0]
        
    engine = create_engine(connection_uri) 
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
