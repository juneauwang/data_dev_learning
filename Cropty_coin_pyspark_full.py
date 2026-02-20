import json
from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pandas as pd
import numpy as np


# --- 全局常量配置 ---
S3_BUCKET = "data-platform-university-labs"
GLUE_DATABASE = "crypto_db"  # 请确保你在 AWS Glue Console 已经创建了这个库
ICEBERG_CATALOG = "glue_catalog"

def get_spark_session(app_name):
    """
    辅助函数：在每个 Task 内部调用，确保 Spark 环境隔离且配置一致
    """
    from pyspark.sql import SparkSession
    import os
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
    
    # 动态获取 AWS 凭证 (从 Airflow Connection 'aws_s3_conn' 拿)
    aws_hook = AwsGenericHook(aws_conn_id='aws_s3_conn')
    creds = aws_hook.get_credentials()
    
    ICEBERG_VERSION = "1.4.2"
    SPARK_VERSION = "3.4"
    os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
    os.environ["AWS_REGION"] = "us-east-1"
    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.12:{ICEBERG_VERSION}",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
    ]

    return SparkSession.builder \
        .appName(app_name) \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.client.region", "us-east-1") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint", "https://s3.us-east-1.amazonaws.com") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.access-key", creds.access_key) \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.secret-key", creds.secret_key) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", f"s3a://{S3_BUCKET}/iceberg-warehouse") \
        .config("spark.hadoop.fs.s3a.access.key", creds.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .getOrCreate()

@dag(
    dag_id='crypto_lakehouse_pipeline_v2',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pyspark', 'iceberg', 'glue']
)
def crypto_lakehouse_pipeline():

    @task
    def task_bronze_ingest_crypto():
        """
        Step 1: 从 CoinGecko 获取原始数据并存入 S3 Bronze 层 (JSON)
        """
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        file_key = f"bronze/crypto_top100_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        
        aws_hook = AwsGenericHook(aws_conn_id='aws_s3_conn',client_type='s3')
        s3_client = aws_hook.get_conn()
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=file_key,
            Body=json.dumps(data)
        )
        return file_key

    @task
    def task_silver_spark_quant_transform(bronze_file_key):
        """
        Step 2: 读取 Bronze JSON，清洗并重命名，存入 Glue Iceberg Silver 表
        """
        from pyspark.sql.functions import col, to_timestamp
        
        spark = get_spark_session("CryptoSilverTransform")
        
        # 1. 读取原始 JSON
        df_raw = spark.read.json(f"s3a://{S3_BUCKET}/{bronze_file_key}")
        
        # 2. 清洗并重命名 (核心修改点：alias)
        df_silver = df_raw.select(
            col("id"),
            col("symbol"),
            col("name"),
            col("current_price"),
            col("market_cap"),
            col("total_volume"),
            col("price_change_percentage_24h").alias("pct_change_24h"), # 改名
            to_timestamp(col("last_updated")).alias("updated_at")
        )
        
        # 3. 写入 Glue Catalog
        #df_silver.writeTo(f"{ICEBERG_CATALOG}.{GLUE_DATABASE}.crypto_silver").createOrReplace()
        df_silver.write.format("iceberg").mode("append").saveAsTable(f"{ICEBERG_CATALOG}.{GLUE_DATABASE}.crypto_silver")
        
        spark.stop()
        return f"{GLUE_DATABASE}.crypto_silver"

    @task
    def task_gold_spark_analysis(silver_table_path):
        """
        Step 3: 读取 Silver 表，计算量化指标，存入 Glue Iceberg Gold 表
        """
        from pyspark.sql.functions import col, sum as _sum, round as _round, when, abs as _abs
        
        spark = get_spark_session("CryptoGoldAnalysis")
        
        # 1. 从 Glue 读取 Silver
        df = spark.table(f"{ICEBERG_CATALOG}.{silver_table_path}")
        
        # 2. 量化计算逻辑
        total_mkt_cap = df.select(_sum("market_cap")).collect()[0][0]
        
        gold_df = df.withColumn(
            "mkt_cap_weight", _round((col("market_cap") / total_mkt_cap) * 100, 4)
        ).withColumn(
            "volatility_rank",
            when(_abs(col("pct_change_24h")) >= 10, "Extreme") # 使用 Silver 重命名后的名字
            .when(_abs(col("pct_change_24h")) >= 5, "High")
            .otherwise("Stable")
        ).select(
            "id", "symbol", "current_price", "mkt_cap_weight", "volatility_rank", "updated_at"
        )
        
        # 3. 写入 Gold 表
        #gold_df.writeTo(f"{ICEBERG_CATALOG}.{GLUE_DATABASE}.crypto_gold_metrics").createOrReplace()
        gold_df.write.format("iceberg").mode("append").saveAsTable(f"{ICEBERG_CATALOG}.{GLUE_DATABASE}.crypto_gold_metrics")
        
        print("✅ Gold 层分析完成，已在 Athena 中可用")
        gold_df.show(10)
        
        spark.stop()
    
    @task(task_id="analyze_crypto_correlation")
    
    def task_analyze_correlation():
        # 注意：这里的连接信息可以存在 Airflow Connection 里以保安全
        # 也可以直接写在这里测试
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        import os
        s3_staging = "s3://data-platform-university-labs/athena_result/ " 
        aws_hook = AwsBaseHook(aws_conn_id='aws_s3_conn', client_type='athena')
        creds = aws_hook.get_credentials()
        os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
        os.environ["AWS_REGION"] = "us-east-1"
        cursor = connect(
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            s3_staging_dir=s3_staging,
            region_name="us-east-1",
            cursor_class=PandasCursor
        ).cursor()

        query = """
        SELECT updated_at, symbol, current_price
        FROM crypto_db.crypto_silver
        WHERE updated_at > current_timestamp - interval '24' hour
        """
        
        df = cursor.execute(query).as_pandas()
        
        if df.empty:
            print("No data found.")
            return

        # 数据处理
        pivot_df = df.pivot_table(index='updated_at', columns='symbol', values='current_price')
        log_returns = np.log(pivot_df).diff().dropna()
        
        # 计算与 BTC 的相关性
        if 'btc' in log_returns.columns:
            corr_series = log_returns.corr()['btc'].drop('btc').sort_values(ascending=False)
            
            # 重点：打印到日志，Airflow 的 Logs 就能看到结果
            print("-" * 30)
            print("TOP CORRELATED WITH BTC:")
            print(corr_series.head(10))
            print("\nTOP ANTI-CORRELATED WITH BTC:")
            print(corr_series.tail(10))
            print("-" * 30)
        else:
            print("BTC not found in symbols.")

    # 1. 抓取原始数据
    bronze_key = task_bronze_ingest_crypto()
    
    # 2. Spark 清洗存入 Silver
    silver_status = task_silver_spark_quant_transform(bronze_key)
    
    # 3. 原有的 Gold 聚合（继续保留，用于 Grafana 看板）
    gold_status = task_gold_spark_analysis(silver_status)

    # 4. 新增的 NumPy 量化分析 (接在 Silver 后面)
    # 即使不把 silver_status 传进去，>> 也能保证顺序
    analysis_report = task_analyze_correlation()
    
    # 设置依赖关系
    silver_status >> gold_status
    silver_status >> analysis_report

# 实例化 DAG
crypto_dag = crypto_lakehouse_pipeline()