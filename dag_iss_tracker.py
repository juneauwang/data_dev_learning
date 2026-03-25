from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import requests
import json
import datetime
import time

# 配置参数
BOOTSTRAP_SERVERS = ['kafka-headless:9092']
TOPIC_NAME = 'iss_raw'

def produce_iss_data():
    # 1. 初始化 Producer
    # value_serializer 自动把 dict 转成 JSON 字节流，省去手动 encode
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1, # SRE 经验：acks=1 在性能和可靠性间平衡最好
        retries=3
    )

    try:
        # 2. 抓取 ISS 实时 API
        # 模拟高频：在一个 Task 任务周期内抓取多次，或者只抓一次由 Airflow 调度
        response = requests.get("http://api.open-notify.org/iss-now.json", timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # 3. 发送数据
            future = producer.send(TOPIC_NAME, value=data)
            
            # 等待发送确认（这一步能确保数据真的进了 Kafka）
            record_metadata = future.get(timeout=10)
            print(f"✅ 已发送至 Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
        else:
            print(f"❌ API 报错: {response.status_code}")
            
    except Exception as e:
        print(f"🚨 生产者运行异常: {e}")
    finally:
        # 必须 close，否则可能会丢失内存中 buffer 的数据
        producer.close()

# 定义 DAG
with DAG(
    'iss_kafka_ingestion',
    default_args={'owner': 'wpwang'},
    start_date=datetime.datetime(2026, 3, 25),
    schedule_interval='* * * * *', # 每分钟运行
    catchup=False,
    tags=['SRE', 'ISS', 'Kafka']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_iss_and_push_to_kafka',
        python_callable=produce_iss_data
    )
