from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.sql import SQLCheckOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import requests
import logging
from datetime import timedelta

# --- 1. 逻辑函数定义 ---
def fetch_and_save_data():
    """
    负责从 API 抓取并批量 Upsert 进 ODS 层
    """
    api_url = "http://universities.hipolabs.com/search?country=China"
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    response = requests.get(api_url, timeout=20)
    response.raise_for_status()
    data = response.json()
    
    # 转换为元组列表
    rows = [
        (uni.get('name'), uni.get('alpha_two_code'), uni.get('country'))
        for uni in data
    ]
    
    # 使用批量插入并处理冲突（replace=True 对应 Postgres 的 ON CONFLICT）
    pg_hook.insert_rows(
        table='raw_users',
        rows=rows,
        target_fields=['external_id', 'username', 'email'],
        replace=True,
        replace_index='external_id'
    )
    logging.info(f"Successfully ingested {len(rows)} records into Bronze/Raw layer.")

# --- 2. DAG 定义 ---
default_args = {
    'owner': 'data_infra',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'dag_full_pipeline_v2',
    default_args=default_args,
    description='End-to-End University Data Pipeline (Medallion Architecture)',
    schedule_interval='@daily', # 每天凌晨 0 点运行
    start_date=days_ago(5),
    catchup=True,
    template_searchpath='/tmp',
    max_active_runs=1,      # 补数时，一次只跑一个日期，防止把数据库压垮
) as dag:

    # 任务 A: 初始化环境 (DDL)
    # 生产中建议放在单独的迁移脚本或独立的 TaskGroup 中
    init_db = PostgresOperator(
        task_id='init_tables',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS raw_users (
            id SERIAL PRIMARY KEY,
            external_id TEXT UNIQUE,
            username TEXT,
            email TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS dwd_universities (
            id SERIAL PRIMARY KEY,
            university_name TEXT,
            country_code TEXT,
            country_name TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS ads_university_count (
            country_name TEXT,
            total_count INT,
            report_date DATE DEFAULT '{{ ds }}'
        );
        """
    )

    # 任务 B: 数据抓取 (Bronze Layer)
    ingest_raw = PythonOperator(
        task_id='ingest_bronze_api',
        python_callable=fetch_and_save_data,
    )

    # 任务 C: 数据质量检查 (Guardrail)
    # 如果 raw_users 里没数据，后面的 SQL 就不跑了，省资源且防错
    check_raw = SQLCheckOperator(
        task_id='check_bronze_quality',
        conn_id='postgres_default',
        sql="SELECT COUNT(*) FROM raw_users;",
    )

    # 任务 D: 数仓建模 (Silver & Gold Layer)
    with TaskGroup("modelling_layers") as modelling:
        
        silver_transform = PostgresOperator(
            task_id='ods_to_dwd_incremental',
            postgres_conn_id='postgres_default',
            sql="""
-- 1. 插入新数据或更新旧数据（使用 UPSERT 逻辑）
            INSERT INTO dwd_universities (university_name, country_code, country_name, processed_at)
            SELECT DISTINCT 
                TRIM(external_id), 
                UPPER(username), 
                email,
                CURRENT_TIMESTAMP
            FROM raw_users
            -- 核心增量逻辑：只选出在本批次时间内进入 raw_users 的数据
            WHERE ingested_at >= '{{ data_interval_start }}' 
              AND ingested_at < '{{ data_interval_end }}'
            ON CONFLICT (university_name) DO UPDATE SET
                country_code = EXCLUDED.country_code,
                processed_at = CURRENT_TIMESTAMP;
            """
        )

        gold_transform = PostgresOperator(
            task_id='dwd_to_ads_gold_backfill',
            postgres_conn_id='postgres_default',
            sql="""
-- 1. 幂等性清理
            DELETE FROM ads_university_count WHERE report_date = '{{ ds }}';

            -- 2. 基于逻辑日期 (ds) 的动态统计
            INSERT INTO ads_university_count (country_name, total_count, report_date)
            SELECT 
                country_name, 
                COUNT(*), 
                '{{ ds }}'::DATE 
            FROM dwd_universities 
            -- 即使是补数，也只统计该日期及以前的数据
            WHERE processed_at::DATE <= '{{ ds }}'::DATE 
            GROUP BY country_name;
            """
        )
        
        silver_transform >> gold_transform

    # --- 编排链路 ---
    init_db >> ingest_raw >> check_raw >> modelling
