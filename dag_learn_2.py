from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def fetch_weather():
    # 模拟 API 调用
    return random.choice(["Sunny", "Rainy", "Storm"])

def check_alert(**context):
    weather = context['ti'].xcom_pull(task_ids='get_weather')
    threshold = Variable.get("weather_alert_level", default_var="Storm")
    
    if weather == threshold:
        print(f"⚠️ 警告：当前天气 {weather} 触发预警阈值！")
    else:
        print(f"✅ 天气 {weather} 正常。")

with DAG('weather_study_dag', start_date=datetime(2026, 2, 1), schedule='@hourly', catchup=False) as dag:
    t1 = PythonOperator(task_id='get_weather', python_callable=fetch_weather)
    t2 = PythonOperator(task_id='check_alert', python_callable=check_alert)
    
    t1 >> t2
