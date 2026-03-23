from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
from clickhouse_driver import Client

# 1. 配置参数
# 建议在 Airflow UI 的 Admin -> Variables 里设置 nasa_api_key
NASA_API_KEY = Variable.get("nasa_api_key", default_var="DEMO_KEY")

# ClickHouse 连接配置 (根据你的 SRE 集群实际 Service 名称修改)
CH_HOST = "clickhouse-headless"
CH_PORT = 9000

default_args = {
    'owner': 'wpwang_sre',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 22),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

def sync_apod_to_clickhouse():
    # A. 请求 NASA APOD 接口
    url = f"https://api.nasa.gov/planetary/apod?api_key={NASA_API_KEY}"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()
    
    # 我们只存图片，不存视频（有时候 NASA 会发科普视频）
    if data.get('media_type') != 'image':
        print(f"Today's media is {data.get('media_type')}, skipping.")
        return

    # B. 连接 ClickHouse
    client = Client(host=CH_HOST, port=CH_PORT)
    
    # C. 执行写入
    # 使用 INSERT 语句，注意字段要和我们之前建的表对齐
    sql = """
    INSERT INTO nasa_apod (date, title, url, hdurl, explanation, media_type)
    VALUES
    """
    row = [(
        datetime.strptime(data['date'], '%Y-%m-%d').date(),
        data['title'],
        data['url'],
        data.get('hdurl', data['url']),
        data.get('explanation', ''),
        data['media_type']
    )]
    
    client.execute(sql, row)
    print(f"Successfully synced APOD: {data['title']}")

# 2. 定义 DAG
with DAG(
    'nasa_apod_wallpaper_v1',
    default_args=default_args,
    description='Syncs daily NASA APOD image to ClickHouse',
    schedule_interval='0 10 * * *', # 建议每天上午 10 点跑，确保 NASA 已更新
    catchup=False,
    tags=['nasa', 'sre_visual']
) as dag:

    sync_task = PythonOperator(
        task_id='fetch_and_save_apod',
        python_callable=sync_apod_to_clickhouse
    )
