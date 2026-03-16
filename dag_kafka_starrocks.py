from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from kafka import KafkaProducer
import json

def fetch_and_push():
    # 真实 API 示例 (例如 CoinGecko)
    api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    response = requests.get(api_url).json()
    
    producer = KafkaProducer(
        bootstrap_servers=['kafka-0.kafka-headless:9092', 'kafka-1.kafka-headless:9092', 'kafka-2.kafka-headless:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # 转换 API 数据格式并发送
    for coin, data in response.items():
        payload = {
            "id": coin,
            "symbol": coin[:3].upper(),
            "name": coin.capitalize(),
            "current_price": data['usd'],
            "market_cap": 0, # API 若无则设默认值
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send('crypto_topic', payload)
    
    producer.flush()

with DAG('crypto_realtime_pipeline', start_date=datetime(2026, 3, 16), schedule_interval='* * * * *') as dag:
    task = PythonOperator(task_id='fetch_crypto_data', python_data=fetch_and_push)
