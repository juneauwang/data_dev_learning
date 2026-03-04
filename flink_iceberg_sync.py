from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
import requests
import time
import os

# --- 配置区 ---
# 如果 Airflow 和 Flink 在同一个 K8s Namespace，直接用 Service 名
# 如果你还是在本地开发环境调试，先用 localhost (配合你的 port-forward)
FLINK_GATEWAY_URL = "http://my-first-flink-cluster-rest:8083/v1" 
AWS_CONN_ID = "aws_default" # 你在 Airflow UI 里定义的 Connection ID

DEFAULT_ARGS = {
    'owner': 'sre',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_flink_sql_task(**kwargs):
    session_url = f"{FLINK_GATEWAY_URL}/sessions"
    aws_hook = AwsGenericHook(aws_conn_id='aws_s3_conn')
    credentials = aws_hook.get_credentials()
    os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
    os.environ["AWS_REGION"] = "us-east-1"
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    # 1. 创建 Session
    resp = requests.post(session_url, json={"sessionName": "airflow_iceberg_job"})
    session_handle = resp.json()["sessionHandle"]
    print(f"Connected to Flink. Session: {session_handle}")

# 2. 动态构建 SQL (使用从 Hook 获取的 AK/SK)
    create_catalog_sql = f"""
    CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
      'type'='iceberg',
      'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
      'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
      'warehouse'='s3://data-platform-university-labs/iceberg-warehouse/',
      's3.access-key' = '{access_key}',
      's3.secret-key' = '{secret_key}',
      's3.endpoint' = 's3.amazonaws.com'
    )
    """
    statements = [
        create_catalog_sql,
        "USE CATALOG iceberg_catalog",
        "SHOW TABLES" # 或者你的同步语句
    ]

    for sql in statements:
        print(f"Executing: {sql[:50]}...")
        stmt_url = f"{session_url}/{session_handle}/statements"
        op_resp = requests.post(stmt_url, json={"statement": sql})
        op_handle = op_resp.json()["operationHandle"]

        # 轮询直到完成
        status_url = f"{session_url}/{session_handle}/operations/{op_handle}/status"
        while True:
            status = requests.get(status_url).json()["status"]
            if status == "FINISHED":
                break
            if status == "ERROR":
                err_resp = requests.get(f"{session_url}/{session_handle}/operations/{op_handle}/result").json()
                print(f"❌ 详细错误原因: {json.dumps(err_resp, indent=2)}")
                raise Exception(f"SQL Execution Failed: {sql}")
            time.sleep(3)
    
    print("All Flink tasks finished successfully!")

with DAG(
    'flink_iceberg_automation',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, # 手动触发
    catchup=False
) as dag:

    sync_iceberg_to_ch = PythonOperator(
        task_id='sync_iceberg_to_clickhouse',
        python_callable=run_flink_sql_task
    )