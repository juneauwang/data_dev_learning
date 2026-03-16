from kafka import KafkaProducer
import json
import time
import random

# 连接你的 K8s 内部 Kafka 集群
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_crypto_data():
    data = {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 70000 + random.randint(-100, 100),
        "market_cap": 1400000000000,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send('crypto_topic', data)
    print(f"Sent: {data}")

while True:
    produce_crypto_data()
    time.sleep(2)
