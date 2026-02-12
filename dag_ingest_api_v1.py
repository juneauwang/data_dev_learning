from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import requests
import json
import logging
from datetime import timedelta
import random
# --- 1. 定义失败时的通知函数 ---
def on_failure_callback(context):
    """
    当任务失败时，Airflow 会调用这个函数。
    你可以在这里对接钉钉、飞书或邮件。
    """
    task_id = context.get('task_instance').task_id
    err_msg = context.get('exception')
    logging.error(f"🚨 任务 {task_id} 挂了！错误信息: {err_msg}")

# --- 2. 抓取并存储数据的逻辑 ---
def fetch_and_save_data():
    # 模拟一个外部 API：这里用随机用户接口作为数据源
#    if random.random() < 0.5:
#        logging.info("🎲 运气不好，模拟触发网络异常...")
#        raise ConnectionError("🌐 模拟网络连接失败！Airflow 应该准备重试...")
    api_url = "http://universities.hipolabs.com/search?country=China" 
    
    response = requests.get(api_url, timeout=10)
    response.raise_for_status() # 如果状态码不是 200，直接抛出异常触发重试
    
    data = response.json()
    logging.info(f"DEBUG: 抓取成功，准备解析 {len(data)} 条记录")
#    sample_data = data[:10]
    #users = data['results']
    rows_to_insert = [
	(uni.get('name'),uni.get('alpha_two_code'),uni.get('country'))
	for uni in data
    ]     
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    logging.info(f"DEBUG: 元组列表构建完成，准备写入数据库")
    # 准备写入数据库
# --- 3. 写入数据库 ---
    pg_hook.insert_rows(
        table='raw_users',
        rows=rows_to_insert,
        target_fields=['external_id', 'username', 'email'],
        commit_every=100,
        replace=True,          # <--- 就是这行
        replace_index='external_id' # <--- 必须告诉它哪个字段是冲突判断的“唯一索引”
        # 批量写入时的冲突处理比较复杂，通常我们会先写入临时表（Staging Table）
    )
    logging.info(f"✅ 成功搬运 {len(sample_data)} 条大学数据！")
# --- 3. 定义 DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 【核心配置：重试机制】
    'retries': 3,                            # 失败后重试 3 次
    'retry_delay': timedelta(seconds=30),     # 每次重试间隔 30 秒
    'on_failure_callback': on_failure_callback # 失败时调用的函数
}

with DAG(
    'dag_api_ingestion_v2',
    default_args=default_args,
    description='从外部 API 抓取 JSON 并存入 Postgres',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 任务 1：准备表结构
    prepare_table = PostgresHook(postgres_conn_id='postgres_default').run("""
        CREATE TABLE IF NOT EXISTS raw_users (
            id SERIAL PRIMARY KEY,
            external_id TEXT UNIQUE,
            username TEXT,
            email TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """) # 注意：这里为了演示直接写了，实际建议用 PostgresOperator

    # 任务 2：执行抓取
    ingest_task = PythonOperator(
        task_id='fetch_external_users',
        python_callable=fetch_and_save_data,
        # 你也可以在单个任务级别覆盖重试配置
        retries=5, 
    )

    ingest_task
