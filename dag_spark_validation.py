from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os

# 這裡建議使用你之前的 S3 變量
S3_BUCKET_NAME = "data-platform-university-labs" # 改成你的桶名
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
		import os
		from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

	aws_hook = AwsBaseHook(aws_conn_id="aws_s3_conn", client_type="s3")
credentials = aws_hook.get_credentials()
	aws_access_key = credentials.access_key
	aws_secret_key = credentials.secret_key
# 如果你的连接里配置了 token（临时凭证），也拿出来
#aws_session_token = credentials.token
	print("🚀 正在初始化 SparkSession (帶着 Iceberg 配件)...")

# 關鍵配置：這決定了 Spark 能不能玩轉 Iceberg
	spark = SparkSession.builder \
		.appName("SparkIcebergTest") \
		.config("spark.jars.packages", 
				"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,"
				"org.apache.hadoop:hadoop-aws:3.3.4") \
			.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
			.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
			.config("spark.sql.catalog.local.type", "hadoop") \
			.config("spark.sql.catalog.local.warehouse", f"s3a://data-platform-university-labs/iceberg-warehouse") \
# --- AWS S3 专属配置 ---
			.config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
				.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
				.config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
				.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
# 启用 AWS SDK 默认的凭证提供者鏈（可选，如果你想用 IAM Role）
				.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
					.getOrCreate()

					print("✅ SparkSession 初始化成功！")

# 1. 測試：創建一個簡單的 DataFrame 並寫入 Iceberg 表
					data = [("China", 398), ("USA", 500), ("Japan", 200)]
	columns = ["country", "university_count"]
test_df = spark.createDataFrame(data, columns)

	print("📝 正在嘗試寫入 Iceberg 表...")
# 在 local catalog 下創建一個名為 test_table 的表
	test_df.writeTo("local.db.test_iceberg_table") \
		.tableProperty("format-version", "2") \
		.createOrReplace()

		print("🎉 Iceberg 表寫入成功！")

# 2. 測試：讀取剛才寫入的表
	read_df = spark.table("local.db.test_iceberg_table")
read_df.show()

	print(f"📈 讀取成功，總行數: {read_df.count()}")

spark.stop()

test_pyspark_iceberg()

dag_spark_iceberg_validation_instance = dag_spark_iceberg_validation()
