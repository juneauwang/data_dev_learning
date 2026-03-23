from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
from kafka import KafkaProducer

# 1. 从 Airflow Variable 安全获取 Key
NASA_API_KEY = Variable.get("nasa_api_key")

default_args = {
    'owner': 'wpwang',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_nasa_neo_to_kafka():
    today = datetime.now().strftime('%Y-%m-%d')
    # 使用从 Variable 获取的 Key
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={today}&end_date={today}&api_key={NASA_API_KEY}"
    
    # 模拟 SRE 环境中的 Kafka 地址 (k8s 内部域名)
    producer = KafkaProducer(
        bootstrap_servers=['kafka-headless:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 提取今天的小行星列表
        asteroids = data.get('near_earth_objects', {}).get(today, [])
        
        for obj in asteroids:
            payload = {
                "neo_reference_id": obj['neo_reference_id'],
                "name": obj['name'],
                "kilometers_max_diameter": obj['estimated_diameter']['kilometers']['estimated_diameter_max'],
                "is_potentially_hazardous": 1 if obj['is_potentially_hazardous_asteroid'] else 0,
                "close_approach_date": obj['close_approach_data'][0]['close_approach_date'],
                "velocity_km_h": float(obj['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']),
                "miss_distance_km": float(obj['close_approach_data'][0]['miss_distance']['kilometers'])
            }
            # 发送到原始 Topic
            producer.send('asteroid_raw', payload)
            
        producer.flush()
        print(f"Successfully pushed {len(asteroids)} asteroids to Kafka.")
        
    except Exception as e:
        print(f"Error fetching NASA data: {e}")
        raise

with DAG(
    'nasa_planetary_defense_v1',
    default_args=default_args,
    description='NASA NeoWs to Kafka Pipeline',
    schedule_interval='@daily', # 每天跑一次，NASA 数据每天更新
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_asteroids_from_nasa',
        python_callable=fetch_nasa_neo_to_kafka
    )
