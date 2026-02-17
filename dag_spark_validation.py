from airflow.decorators import dag, task
from datetime import datetime
import os

# 這裡建議使用你之前的 S3 變量
S3_BUCKET_NAME = "data-platform-university-labs"
S3_CONN_ID = "aws_s3_conn"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'iceberg', 'test']
)
def dag_spark_iceberg_validation():

    @task
    def test_pyspark_iceberg(ds=None):
        from pyspark.sql import SparkSession
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        import os

        # 這裡的代碼必須比 def 縮進更多
        aws_hook = AwsBaseHook(aws_conn_id="aws_s3_conn", client_type="s3")
        credentials = aws_hook.get_credentials()
        aws_access_key = credentials.access_key
        aws_secret_key = credentials.secret_key
        
        print("🚀 正在初始化 SparkSession (帶着 Iceberg 配件)...")

        # 關鍵配置
        spark = SparkSession.builder \
            .appName("SparkIcebergTest") \
            .config("spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,"
                    "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", f"s3a://{S3_BUCKET_NAME}/iceberg-warehouse") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()

        print("✅ SparkSession 初始化成功！")

        # 1. 測試：寫入
        data = [("China", 398), ("USA", 500), ("Japan", 200)]
        columns = ["country", "university_count"]
        test_df = spark.createDataFrame(data, columns)

        print("📝 正在嘗試寫入 Iceberg 表...")
        test_df.writeTo("local.db.test_iceberg_table") \
            .tableProperty("format-version", "2") \
            .createOrReplace()

        print("🎉 Iceberg 表寫入成功！")

        # 2. 測試：讀取
        read_df = spark.table("local.db.test_iceberg_table")
        read_df.show()
        print(f"📈 讀取成功，總行數: {read_df.count()}")

        spark.stop()

    # 這裡調用 task
    test_pyspark_iceberg()

# 實例化 DAG
dag_spark_iceberg_validation_instance = dag_spark_iceberg_validation()
