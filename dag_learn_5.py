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
    do_analysis = PythonOperator(
        task_id='run_analysis',
        python_callable=process_new_data
    )

    wait_for_data >> do_analysis
