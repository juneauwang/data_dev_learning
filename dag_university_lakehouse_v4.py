from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import io
from airflow.exceptions import AirflowFailException
import random
import time
# 配置信息
S3_BUCKET_NAME = "data-platform-university-labs" # <--- 修改这里
#S3_BUCKET_NAME = "data-platform-university-test"
S3_CONN_ID = "aws_s3_conn"            # 这是你在 UI 里创建的 Connection ID

with DAG(
    'dag_university_lakehouse_v4',
    default_args={
	'owner': 'airflow',
    	'email': ['wangweipeng_nob@163.com'],
    	'email_on_failure': True, # 失败时发邮件
    	'email_on_retry': False,
    	'retries': 1,},
    start_date=days_ago(30),     # 开启回溯，跑过去 3 天的数据
    catchup=True,               # 核心：开启补数
    schedule_interval='@daily',
    max_active_runs=1           # 保证顺序执行，避免数据库冲突
) as dag:


    @task
    def validate_data_quality(s3_key, bucket_name):
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    # 关键：不要直接 read_key，因为那会触发自动 decode
    # 我们先获取 S3 对象
        file_obj = s3.get_key(s3_key, bucket_name)
        file_content = file_obj.get()['Body'].read()

    	# 读回数据进行检查 (或者直接用前面的 DF 长度)
        df = pd.read_parquet(io.BytesIO(file_content))
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
        if random.random() > 0.5:
            df = df.iloc[:random.randint(350, 398)]
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
    @task
    def ads_reporting(ds=None, **kwargs):
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import time
    # 1. 从 Athena 获取统计结果
        athena_hook = AthenaHook(aws_conn_id=S3_CONN_ID)
    # 注意：这里查的是你 Athena 里的表名 university_lake
        query = f"""
            SELECT 'China' as country_name, COUNT(*) as total_count, CAST('{ds}' AS DATE) as report_date
            FROM university_lake
            WHERE execution_date = '{ds}'
            GROUP BY 1
        """
    
    # 这里的 database 是你在 Athena 创建表时用的那个（比如 default）
# 2. 运行查询并等待结果
    # Athena 是异步的，我们需要运行并拿到 QueryExecutionId
        query_execution_context = {"Database": "default"}
        result_config = {"OutputLocation": f"s3://{S3_BUCKET_NAME}/athena_results/"}
    
        execution_id = athena_hook.run_query(
            query, 
            query_context=query_execution_context, 
            result_configuration=result_config
        ) 
    
    # 等待查询完成（简单轮询）
# 2. 手动轮询状态 (解决 Hook 缺少 backend 方法的问题)
        print(f"正在等待 Athena 查询完成: {execution_id}")
        while True:
              # 直接调用底层的 get_conn().get_query_execution
            response = athena_hook.get_conn().get_query_execution(QueryExecutionId=execution_id)
            state = response['QueryExecution']['Status']['State']
        
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        if state != 'SUCCEEDED':
            reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise ValueError(f"Athena 查询失败! 状态: {state}, 原因: {reason}")
    # 3. 获取结果并解析
        results = athena_hook.get_query_results(execution_id)
    
    # Athena 返回的结果结构比较深，我们需要提取出数值
    # 结果示例: [{'Data': [{'VarCharValue': 'China'}, {'VarCharValue': '398'}, {'VarCharValue': '2026-02-12'}]}]
        rows = results['ResultSet']['Rows']
    
        if len(rows) <= 1: # 只有表头或者没数据
            print(f"警告：{ds} 在 Athena 中没有查到记录")
            return

    # 提取第一行数据（跳过表头）
        data_row = rows[1]['Data']
        country = data_row[0]['VarCharValue']
        count = int(data_row[1]['VarCharValue'])
        r_date = data_row[2]['VarCharValue']

    # 4. 写入 Postgres
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        pg_hook.run(f"DELETE FROM ads_university_count WHERE report_date = '{ds}';")
    
        insert_sql = "INSERT INTO ads_university_count (country_name, total_count, report_date) VALUES (%s, %s, %s)"
        pg_hook.run(insert_sql, parameters=(country, count, r_date))
    
        print(f"成功将 {ds} 的数据 ({country}: {count}) 写入 Postgres")


# 1. 运行入库任务
    s3_file_reference = ingest_to_s3_parquet()
    reporting = ads_reporting()
# 2. 运行校验任务，并显式传参
# 这一步会解决你的 "missing a required argument: s3_key" 报错
    check_data = validate_data_quality(
    	s3_key=s3_file_reference, 
    	bucket_name=S3_BUCKET_NAME
    )

# 3. 设置依赖链
    s3_file_reference >> check_data >> reporting
