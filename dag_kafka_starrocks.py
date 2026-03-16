import os
import json
import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

# 获取日志记录器
logger = logging.getLogger("airflow.task")

def fetch_and_push():
    # 从环境变量获取配置，默认为本地开发地址
    kafka_servers = Variable.get("kafka_bootstrap_servers", default_var="kafka-0.kafka-headless:9092,kafka-1.kafka-headless:9092,kafka-2.kafka-headless:9092")

 
    try:
        api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
        response = requests.get(api_url, timeout=10)
        response.raise_for_status() # 检查 HTTP 错误
        data_json = response.json()
        
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for coin, data in data_json.items():
            payload = {
                "id": coin,
                "symbol": coin[:3].upper(),
                "name": coin.capitalize(),
                "current_price": data['usd'],
                "market_cap": 0,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            producer.send('crypto_topic', payload)
            logger.info(f"Sent {coin} data to Kafka")

        producer.flush()
        producer.close()
        logger.info("Successfully pushed all data to Kafka.")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise e # 抛出异常使任务在 Airflow 中显示为 failed

with DAG(
    dag_id='crypto_realtime_pipeline', 
    start_date=datetime(2026, 3, 16), 
    schedule_interval='* * * * *',
    catchup=False # 通常实时任务不需要补录历史数据
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_crypto_data', 
        python_callable=fetch_and_push
    )
