from airflow.decorators import dag, task
from datetime import datetime
import json

# 配置常量
S3_BUCKET = "data-platform-university-labs"
S3_CONN_ID = "aws_s3_conn"
BRONZE_KEY = "bronze/universities/all_universities_raw.json"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

@dag(
    default_args=default_args,
    schedule_interval=None,  # 手动触发
    catchup=False,
    tags=['full_load', 'iceberg', 'silver']
)
def university_full_load_pipeline():

    @task
    def task_bronze_ingest():
        """第一步：全量拉取 API 数据并存入 S3 (Bronze 层)"""
        import requests
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        print("📡 正在从 API 获取全量大学数据...")
        url = "http://universities.hipolabs.com/search"
        response = requests.get(url)
        print(f"📡 正在尝试下载全量数据 (使用流式模式)...")
        
        # 增加 headers 模拟浏览器，有时能绕过代理限制
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        try:
            # 增加 timeout=60 防止无限挂起
            response = requests.get(url, headers=headers, timeout=60, stream=True)
            response.raise_for_status() # 如果 404 或 500 会直接报错
            
            # 使用 content 而不是 .json()，因为 .json() 在断流时会崩
            full_content = response.content
            
            # 验证 JSON 完整性
            data = json.loads(full_content)
            print(f"✅ 下载成功！记录数: {len(data)}")

            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
            s3_hook.load_string(
                string_data=json.dumps(data),
                key=BRONZE_KEY,
                bucket_name=S3_BUCKET,
                replace=True
            )
            return BRONZE_KEY

        except Exception as e:
            print(f"❌ 下载失败: {e}")
            raise  # 抛出异常让 Airflow 重试

    @task
    def task_silver_spark_transform(bronze_key):
        """第二步：使用 Spark 读取 Bronze JSON 并写入 Iceberg (Silver 层)"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, array_join
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        # 1. 动态获取 AWS 凭证
        aws_hook = AwsBaseHook(aws_conn_id=S3_CONN_ID, client_type="s3")
        creds = aws_hook.get_credentials()

        # 2. 初始化 Spark
        print("🚀 启动 Spark 引擎...")
        spark = SparkSession.builder \
            .appName("BronzeToSilverFullLoad") \
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
        print(f"📖 正在读取 Bronze 数据: s3a://{S3_BUCKET}/{bronze_key}")
        df = spark.read.option("multiLine", "true").json(f"s3a://{S3_BUCKET}/{bronze_key}")

        # 4. 数据清洗与转换
        # - 展开 web_pages
        # - 统一列名
        # - 去重
        silver_df = df.withColumn("web_page", array_join(col("web_pages"), "; ")) \
                      .withColumnRenamed("state-province", "state_province") \
                      .drop("web_pages") \
                      .drop_duplicates(["name", "country"])

        # 5. 写入 Iceberg Silver 表 (按国家分区)
        print("📝 正在写入 Iceberg Silver 表...")
        silver_df.writeTo("local.db.universities_silver") \
            .tableProperty("format-version", "2") \
            .partitionedBy("country") \
            .createOrReplace()

        print("🎉 Silver 层 Iceberg 表已就绪！")
        spark.stop()

    # 编排工作流
    raw_key = task_bronze_ingest()
    task_silver_spark_transform(raw_key)

# 实例化
university_full_load_pipeline_dag = university_full_load_pipeline()