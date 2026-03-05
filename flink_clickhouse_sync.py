from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import time

# 配置信息
FLINK_GATEWAY_URL = "http://my-first-flink-cluster-rest:8083/v1"

def execute_sql(session_url, session_handle, sql):
    """
    通用 Flink SQL 执行函数，包含轮询状态检查
    """
    print(f"--- Executing SQL ---\n{sql}")
    submit_url = f"{session_url}/{session_handle}/statements"
    
    try:
        res = requests.post(submit_url, json={"statement": sql}, timeout=30).json()
        if "operationHandle" not in res:
            raise Exception(f"Failed to submit SQL: {res}")
            
        op_handle = res["operationHandle"]
        status_url = f"{session_url}/{session_handle}/operations/{op_handle}/status"
        
        while True:
            status_res = requests.get(status_url, timeout=10).json()
            status = status_res["status"]
            if status == "FINISHED":
                print("Result: FINISHED")
                return op_handle
            if status == "ERROR":
                # 捕获详细错误信息
                err_res = requests.get(f"{session_url}/{session_handle}/operations/{op_handle}/result/0").json()
                raise Exception(f"SQL execution error: {err_res}")
            time.sleep(2)
    except Exception as e:
        print(f"Error during SQL execution: {str(e)}")
        raise

def run_sync_job(**kwargs):
    session_url = f"{FLINK_GATEWAY_URL}/sessions"
    
    # 1. 创建 Session
    resp = requests.post(session_url, json={"sessionName": "iceberg_ch_streaming_sync"}).json()
    session_handle = resp["sessionHandle"]
    print(f"Connected to Flink. Session ID: {session_handle}")
    
    try:
        # 2. 动态注册 Catalog (SRE 自愈逻辑)
        # 既然 YAML 已注入环境变量，此处无需显式写 AK/SK
        create_catalog_sql = """
        CREATE CATALOG iceberg_catalog WITH (
            'type'='iceberg',
            'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
            'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
            'warehouse'='s3://data-platform-university-labs/iceberg-warehouse/',
            's3.region'='us-east-1'
        )
        """
        try:
            # 注意：Flink 1.18 的 CREATE CATALOG 有时不识别 IF NOT EXISTS
            # 我们直接用 try-except 包裹，即使已存在报错也会继续
            execute_sql(session_url, session_handle, create_catalog_sql)
        except Exception as e:
            print(f"Catalog registration notice: {e}")

        # 3. 设置运行参数 (必须开启 Checkpoint)
        execute_sql(session_url, session_handle, "SET 'execution.runtime-mode' = 'streaming'")
        execute_sql(session_url, session_handle, "SET 'execution.checkpointing.interval' = '1min'")
        execute_sql(session_url, session_handle, "USE CATALOG iceberg_catalog")

        # 4. 创建 ClickHouse 目标映射表
        # 注意：这里的 url 需要在 K8s 内部能解析到 clickhouse-server
        # 1. 创建 ClickHouse 映射表 (确保字段名和类型匹配)
        create_ch_sink = """
        CREATE TABLE IF NOT EXISTS `default_catalog`.`default_database`.clickhouse_sink (
            `id` STRING,
            `symbol` STRING,
            `current_price` DOUBLE,
            `updated_at` TIMESTAMP_LTZ(6)
        ) WITH (
            'connector' = 'clickhouse',
            'url' = 'jdbc:clickhouse://clickhouse:8123/default',
            'table-name' = 'crypto_prices_sink',
            'username' = 'default',
            'password' = '',
            'sink.batch-size' = '500',
            'sink.flush-interval' = '1000'
        )
        """
        execute_sql(session_url, session_handle, create_ch_sink)

        # 5. 提交异步 INSERT 任务
        # 使用 INSERT INTO 会在 Flink Dashboard 生成一个持久 Job
        insert_sql = """
        INSERT INTO `default_catalog`.`default_database`.clickhouse_sink
        SELECT id, symbol, current_price, updated_at
        FROM `iceberg_catalog`.`crypto_db`.`crypto_silver` 
        /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s') */
        """
        # 注意：execute_sql 里的轮询只管任务是否“提交成功”
        # 对于 INSERT 这种持久任务，提交后会返回 FINISHED
        execute_sql(session_url, session_handle, insert_sql)
        print("Success: Streaming Job has been submitted to Flink cluster.")

    finally:
        # 在流处理场景下，我们通常保持 Session 开启或者让集群自行管理
        print("Workflow finished.")

# --- DAG 定义 ---
with DAG(
    'iceberg_to_clickhouse_v1',
    default_args={'owner': 'sre', 'retries': 0},
    start_date=datetime(2026, 3, 5),
    schedule_interval=None,
    catchup=False,
    tags=['flink', 'iceberg', 'clickhouse']
) as dag:

    sync_task = PythonOperator(
        task_id='sync_iceberg_to_ch',
        python_callable=run_sync_job
    )