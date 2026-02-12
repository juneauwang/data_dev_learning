from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import logging

# 1. 定义 ETL 处理函数
def run_user_stats_etl():
    """
    使用 PostgresHook 执行数据转换和搬运
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 这里的逻辑是：从原始日志表统计用户活跃度，然后“覆盖写入”到统计表
    etl_sql = """
    INSERT INTO user_stats (user_id, activity_count, last_active)
    SELECT user_id, COUNT(*), MAX(created_at)
    FROM user_activity_logs
    GROUP BY user_id
    ON CONFLICT (user_id) DO UPDATE 
    SET activity_count = EXCLUDED.activity_count, 
        last_active = EXCLUDED.last_active;
    """
    
    logging.info("开始执行 ETL 转换...")
    pg_hook.run(etl_sql)
    logging.info("数据搬运成功！")

# 2. 定义 DAG
with DAG(
    dag_id='dag_etl_v1',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['learning', 'etl']
) as dag:

    # 任务 1：初始化表结构（如果不存在）
    # 在真实开发中，通常会有专门的任务负责 DDL（数据定义语言）
    prepare_tables = PostgresOperator(
        task_id='prepare_tables',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS user_activity_logs (
            id SERIAL PRIMARY KEY,
            user_id INT,
            action TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INT PRIMARY KEY,
            activity_count INT,
            last_active TIMESTAMP
        );
        """
    )

    # 任务 2：使用 TaskGroup 组织分析逻辑
    with TaskGroup("analysis_processing") as analysis_group:
        
        # 模拟：在搬运前先进行一些数据检查
        check_data = PythonOperator(
            task_id='pre_check',
            python_callable=lambda: print("🔍 检查原始数据完整性...")
        )

        # 核心：执行 ETL 搬运
        do_etl = PythonOperator(
            task_id='execute_etl',
            python_callable=run_user_stats_etl
        )

        check_data >> do_etl

    # 任务 3：清理或发送通知
    post_cleanup = PostgresOperator(
        task_id='post_cleanup',
        postgres_conn_id='postgres_default',
        sql="-- 这里可以写一些清理旧临时数据的 SQL \n SELECT 1;"
    )

    # 3. 定义任务依赖
    prepare_tables >> analysis_group >> post_cleanup
