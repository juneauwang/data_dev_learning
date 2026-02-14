from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import io
from airflow.exceptions import AirflowFailException
# 配置信息
S3_BUCKET_NAME = "data-platform-university-labs" # <--- 修改这里
S3_CONN_ID = "aws_s3_conn"            # 这是你在 UI 里创建的 Connection ID

with DAG(
    'dag_university_lakehouse_v4',
    default_args={'retries': 1},
    start_date=days_ago(3),     # 开启回溯，跑过去 3 天的数据
    catchup=True,               # 核心：开启补数
    schedule_interval='@daily',
    max_active_runs=1           # 保证顺序执行，避免数据库冲突
) as dag:
    @task
    def validate_data_quality(s3_key, bucket_name):
    	s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    	content = s3.read_key(s3_key, bucket_name)
    
    	# 读回数据进行检查 (或者直接用前面的 DF 长度)
    	df = pd.read_parquet(io.BytesIO(content))
    	row_count = len(df)
    
    	if row_count < 300:
        # 抛出异常，阻止下游任务（Postgres 同步）运行
             raise AirflowFailException(f"数据质量异常！预期 >300，实际仅有 {row_count}")
    
    	print(f"数据质量检查通过：检测到 {row_count} 条记录")
    	return row_count
    @task
    def ingest_to_s3_parquet(ds=None, **kwargs):
        """抓取 API 数据并以 Parquet 格式存入 S3"""
        # 1. API 抓取
        url = "http://universities.hipolabs.com/search?country=China"
        data = requests.get(url).json()
        
        # 2. Pandas 处理
        df = pd.DataFrame(data)
        df = df[['name', 'alpha_two_code', 'country']]
        df.columns = ['uni_name', 'country_code', 'country']
        
        # 3. 内存转换为 Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        # 4. S3Hook 上传
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_key = f"bronze/universities/execution_date={ds}/data.parquet"
        
        s3.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        return s3_key

    # 建模任务：从 DWD 同步到 ADS 报表
    # 模拟“湖”到“仓”的汇总过程
    ads_reporting = PostgresOperator(
        task_id='ads_reporting_upsert',
        postgres_conn_id='postgres_default',
        sql="""
        -- 幂等性删除
        DELETE FROM ads_university_count WHERE report_date = '{{ ds }}';

        -- 模拟从已有的 DWD 表汇总（假设你之前的 V2/V3 任务已存入数据）
        INSERT INTO ads_university_count (country_name, total_count, report_date)
        SELECT country_name, COUNT(*), '{{ ds }}'::DATE
        FROM dwd_universities
        WHERE processed_at::DATE <= '{{ ds }}'::DATE
        GROUP BY country_name;
        """
    )

    # 编排链路
    ingest_to_s3_parquet() >> validate_data_quality()  >> ads_reporting
