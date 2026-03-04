from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsHook # 导入 AWS Hook
from datetime import datetime, timedelta
import requests
import time

# --- 配置区 ---
FLINK_GATEWAY_URL = "http://my-first-flink-cluster-rest:8083/v1" 
AWS_CONN_ID = "aws_s3_conn" # 你在 Airflow UI 里定义的 Connection ID

def run_flink_sql_task(**kwargs):
    # --- 关键步骤：从 Airflow 获取 AWS 凭证 ---
    aws_hook = AwsHook(aws_conn_id=AWS_CONN_ID, client_type="s3")
    credentials = aws_hook.get_credentials()
    
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    # 如果有 session_token (临时凭证)，Flink Iceberg 也支持添加 's3.session-token'
    
    session_url = f"{FLINK_GATEWAY_URL}/sessions"
    
    # 1. 创建 Session
    resp = requests.post(session_url, json={"sessionName": "airflow_iceberg_sync"})
    session_handle = resp.json()["sessionHandle"]

    # 2. 动态构建 SQL (使用从 Hook 获取的 AK/SK)
    create_catalog_sql = f"""
    CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
      'type'='iceberg',
      'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
      'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
      'warehouse'='s3a://data-platform-university-labs/iceberg-warehouse/',
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
        # ... 提交逻辑与之前一致 ...
        print(f"Executing SQL via Gateway...")
        # (此处省略之前的 requests 轮询逻辑，保持代码整洁)