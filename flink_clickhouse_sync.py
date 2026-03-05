from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from datetime import datetime, timedelta
import requests
import time
import json

FLINK_GATEWAY_URL = "http://my-first-flink-cluster-rest:8083/v1"

def execute_sql(session_url, session_handle, sql):
    print(f"Executing: {sql}")
    submit_url = f"{session_url}/{session_handle}/statements"
    res = requests.post(submit_url, json={"statement": sql}).json()
    op_handle = res["operationHandle"]
    
    # 轮询检查状态
    status_url = f"{session_url}/{session_handle}/operations/{op_handle}/status"
    while True:
        status_res = requests.get(status_url).json()
        status = status_res["status"]
        if status == "FINISHED":
            return op_handle
        if status == "ERROR":
            raise Exception(f"SQL Failed: {sql}")
        time.sleep(2)

def run_sync_job():
    session_url = f"{FLINK_GATEWAY_URL}/sessions"
    
    # 1. 创建 Session
    resp = requests.post(session_url, json={"sessionName": "iceberg_clickhouse_sync"}).json()
    session_handle = resp["sessionHandle"]
    
    try:
        # 2. 环境配置：开启 Checkpoint 和 流模式
        execute_sql(session_url, session_handle, "SET 'execution.runtime-mode' = 'streaming'")
        execute_sql(session_url, session_handle, "SET 'execution.checkpointing.interval' = '1min'")

        # 3. 这里的 Catalog 已经在上一个任务建好了，直接 USE 即可
        execute_sql(session_url, session_handle, "USE CATALOG iceberg_catalog")

        # 4. 定义 ClickHouse Sink 表 (如果不存在)
        # 注意：这里的 url 要改成你 K8s 内部 ClickHouse 的 Service 名字
        create_ch_sink = """
        CREATE TABLE IF NOT EXISTS clickhouse_sink (
            `id` STRING,
            `symbol` STRING,
            `price` DOUBLE,
            `ts_ms` BIGINT,
            `dt` STRING
        ) WITH (
            'connector' = 'clickhouse',
            'url' = 'jdbc:clickhouse://clickhouse:8123/default',
            'table-name' = 'crypto_prices',
            'username' = 'default',
            'password' = '',
            'sink.batch-size' = '500'
        )
        """
        execute_sql(session_url, session_handle, create_ch_sink)

        # 5. 执行同步 (从 Iceberg 增量读取)
        # 这里的 OPTIONS 开启了流式监控
        insert_sql = """
        INSERT INTO clickhouse_sink
        SELECT id, symbol, price, ts_ms, dt
        FROM `iceberg_db`.`crypto_table` /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s') */
        """
        execute_sql(session_url, session_handle, insert_sql)
        
    finally:
        # Streaming 任务通常不关闭 Session，或者根据需要手动管理
        print("Sync job submitted to Flink Cluster.")

with DAG(
    'iceberg_to_clickhouse_streaming',
    default_args={'owner': 'sre', 'retries': 0},
    start_date=datetime(2026, 3, 5),
    schedule_interval=None, # 手动触发或按需设置
    catchup=True
) as dag:

    sync_task = PythonOperator(
        task_id='iceberg_to_ch_sync',
        python_callable=run_sync_job
    )