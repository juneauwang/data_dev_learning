from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.sql import SqlSensor
# 定義數據處理邏輯
def process_new_data():
    print("🎯 哨兵報告：檢測到新數據已入庫！正在啟動下游分析程序...")

with DAG(
    'postgres_sensor_v1',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:
# 新增：初始化任务，确保表存在
    create_table = PostgresOperator(
        task_id='create_monitoring_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS user_activity_logs (
                id SERIAL PRIMARY KEY,
                user_id INT,
                action VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )
    # 1. 哨兵任務：每隔 30 秒檢查一次數據庫
    wait_for_data = SqlSensor(
        task_id='wait_for_postgres_data',
        conn_id='postgres_default',
        # 只要 count > 0，Sensor 就會變綠
        sql="SELECT COUNT(*) FROM user_activity_logs WHERE user_id = 999;",
        poke_interval=30,      # 檢查頻率（秒）
        timeout=600,           # 10 分鐘後若還沒數據就超時報錯
        mode='poke'            # 持續佔用 Worker 等待（初學者建議先用這個）
    )

    # 2. 下游處理任務
with TaskGroup("analysis_group", tooltip="數據分析任務組") as analysis_group:
    # 2. 将原来的任务放进组里（注意缩进！）
    do_analysis = PythonOperator(
        task_id='run_analysis',
        python_callable=process_new_data
    )
    
    # 3. 组里可以加更多任务，它们会并排显示
    do_summary = PythonOperator(
        task_id='generate_summary',
        python_callable=lambda: print("📊 摘要：用戶 999 表現活躍！")
    )
    
    create_table >> wait_for_data >> analysis_group
