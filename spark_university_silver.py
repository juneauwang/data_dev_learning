from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array_join

def process_silver():
    # 1. 初始化 Spark (配置已适配 Iceberg)
    spark = SparkSession.builder \
        .appName("UniversitySilverIngest") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://data-platform-university-labs/iceberg-warehouse") \
        .getOrCreate()

    # 2. 读取 Bronze 层的原始 JSON
    # 注意：这里我们读取所有 JSON，或者通过路径传入特定日期的 JSON
    df = spark.read.option("multiLine", "true").json("s3a://data-platform-university-labs/raw/universities/*.json")

    # 3. 数据清洗 (Transform)
    # - 展开 web_pages 数组变成字符串
    # - 重命名带横线的字段，方便 SQL 查询
    cleaned_df = df.withColumn("web_page", array_join(col("web_pages"), "; ")) \
                   .withColumnRenamed("state-province", "state_province") \
                   .drop("web_pages") \
                   .drop_duplicates(["name", "country"])

    # 4. 写入 Iceberg (Silver Table)
    # 使用 createOrReplace 会覆盖全表，如果是增量可以用 mergeInto
    cleaned_df.writeTo("local.db.universities_silver") \
        .tableProperty("format-version", "2") \
        .partitionedBy("country") \
        .createOrReplace()

    print("✅ Silver Layer (Iceberg) 写入完成！")
    spark.stop()

if __name__ == "__main__":
    process_silver()