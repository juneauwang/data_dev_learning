from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    'dag_university_modelling_v1',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['modelling', 'layering']
) as dag:

    # 任务 1：清洗与标准化 (ODS -> DWD)
    # 作用：统一大写，去空格，作为 Managed 表长期保存
    transform_dwd = PostgresOperator(
        task_id='transform_ods_to_dwd',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS dwd_universities (
            id SERIAL PRIMARY KEY,
            university_name TEXT,
            country_code TEXT,
            country_name TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        TRUNCATE TABLE dwd_universities; -- 为了实验简单，我们先做全量覆盖
        
        INSERT INTO dwd_universities (university_name, country_code, country_name)
        SELECT 
            TRIM(external_id), 
            UPPER(username), 
            email
        FROM raw_users;
        """
    )

    # 任务 2：生成统计报表 (DWD -> ADS)
    # 作用：直接产出老板要的“每个国家多少学校”的结论
    create_ads_report = PostgresOperator(
        task_id='generate_country_stats',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS ads_university_count (
            country_name TEXT,
            total_count INT,
            report_date DATE DEFAULT CURRENT_DATE
        );

        TRUNCATE TABLE ads_university_count;

        INSERT INTO ads_university_count (country_name, total_count)
        SELECT 
            country_name, 
            COUNT(*) 
        FROM dwd_universities
        GROUP BY country_name;
        """
    )

    transform_dwd >> create_ads_report
