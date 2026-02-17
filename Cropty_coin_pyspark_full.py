from airflow.decorators import dag, task
from datetime import datetime
import json

# 配置常量 - 以后你就是量化数据工程师了
S3_BUCKET = "data-platform-university-labs"
S3_CONN_ID = "aws_s3_conn"
BRONZE_KEY = "bronze/crypto/markets_top100.json"
# 使用 CoinGecko API 获取市值前 100 的币种
API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

@dag(
    default_args=default_args,
    schedule_interval="@hourly",  # 既然是量化，我们可以每小时跑一次
    catchup=False,
    tags=['crypto', 'iceberg', 'quant']
)
def crypto_lakehouse_pipeline():

    @task(retries=3, retry_delay=30)
    def task_bronze_ingest_crypto():
        """第一步：全量拉取 CoinGecko 数据并存入 S3 (Bronze 层)"""
        import requests
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        print(f"📡 正在从 CoinGecko 获取行情数据...")
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        try:
            # 使用我们定义的 API_URL
            response = requests.get(API_URL, headers=headers, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            print(f"✅ 下载成功！获取到 {len(data)} 个币种行情")

            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
            s3_hook.load_string(
                string_data=json.dumps(data),
                key=BRONZE_KEY,
                bucket_name=S3_BUCKET,
                replace=True
            )
            return BRONZE_KEY

        except Exception as e:
            print(f"❌ Crypto 数据采集失败: {e}")
            raise 

    @task
    def task_silver_spark_quant_transform(bronze_key):
        """第二步：量化清洗，存入 Iceberg Silver 层"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, to_timestamp
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        # 1. 获取 AWS 凭证
        aws_hook = AwsBaseHook(aws_conn_id=S3_CONN_ID, client_type="s3")
        creds = aws_hook.get_credentials()

        # 2. 初始化 Spark
        spark = SparkSession.builder \
            .appName("CryptoBronzeToSilver") \
            .config("spark.jars.packages", 
                    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,"
                    "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", f"s3a://{S3_BUCKET}/iceberg-warehouse") \
            .config("spark.hadoop.fs.s3a.access.key", creds.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
            .getOrCreate()

        # 3. 读取 Bronze JSON
        print(f"📖 正在读取 Crypto 原始数据...")
        df = spark.read.option("multiLine", "true").json(f"s3a://{S3_BUCKET}/{bronze_key}")
        
        # 4. 量化字段清洗
        silver_df = df.select(
            col("id"),
            col("symbol"),
            col("name"),
            col("current_price").cast("double"),
            col("market_cap").cast("long"),
            col("total_volume").cast("long"),
            col("price_change_percentage_24h").alias("pct_change_24h"),
            to_timestamp(col("last_updated")).alias("updated_at")
        ).drop_duplicates(["id", "updated_at"])

        # 5. 写入 Iceberg (按 id 分区)
        print("📝 正在更新 Iceberg Silver 表 (crypto_silver)...")
        silver_df.writeTo("local.db.crypto_silver") \
            .tableProperty("format-version", "2") \
            .partitionedBy("id") \
            .createOrReplace()

        print("🎉 加密货币 Silver 层数据转换完成！")
        spark.stop()

    # 执行流程
    bronze_file = task_bronze_ingest_crypto()
    task_silver_spark_quant_transform(bronze_file)

# 实例化
crypto_dag = crypto_lakehouse_pipeline()