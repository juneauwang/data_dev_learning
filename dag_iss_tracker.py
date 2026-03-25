from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
import requests
import json
import datetime

def produce_iss_data():
    # 1. 抓取 ISS 实时 API
    res = requests.get("http://api.open-notify.org/iss-now.json").json()
    
    # 2. 推送到 Kafka
    p = Producer({'bootstrap.servers': 'data-kafka-0.default.svc.cluster.local:9092'})
    p.produce('iss_raw', json.dumps(res).encode('utf-8'))
    p.flush()

with DAG(
    'iss_stream_producer',
    start_date=datetime.datetime(2026, 3, 25),
    schedule_interval='* * * * *', # 虽然是一分钟调度，但内部可以写循环做高频
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='fetch_and_push',
        python_callable=produce_iss_data
    )
