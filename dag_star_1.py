from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import io
import os
import matplotlib.pyplot as plt
from skyfield.api import load, Star
from skyfield.api import Loader as SkyfieldLoader
from skyfield.data import hipparcos
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

# --- 配置区：对应你 v7 镜像内的路径 ---
ASTRONOMY_DATA_DIR = '/home/airflow/astronomy_data'
BSP_FILE = 'de421.bsp'
HIP_FILE = 'hipparcos.gz'  # 确认你 COPY 进去时改成了这个名字

S3_CONN_ID = 'aws_s3_conn'
S3_BUCKET = 'wpwang-star' # 换成你的桶名
S3_KEY = 'monitoring/summer_triangle_bg.png'

def render_celestial_background(**kwargs):
    """
    核心任务：加载本地天文数据，渲染星图并上传 S3
    """
    # 1. 强制 Headless 模式（防止容器内没有 X11 导致绘图崩溃）
    plt.switch_backend('Agg')
    
    # 2. 加载本地数据
    print(f"🚀 正在从 {ASTRONOMY_DATA_DIR} 加载天文之魂...")
    loader = SkyfieldLoader(ASTRONOMY_DATA_DIR)
    ts = loader.timescale()
    t = ts.now()
    
    planets = loader(BSP_FILE)
    earth = planets['earth']
    
    # 加载星表并筛选夏日大三角 (Vega, Altair, Deneb)
    try:
        with loader.open(HIP_FILE) as f:
            df = hipparcos.load_dataframe(f)
        
        # 对应 HIP ID: 织女星(91262), 牛郎星(97649), 天津四(102098)
        triangle_ids = [91262, 97649, 102098]
        stars = Star.from_dataframe(df.loc[triangle_ids])
    except Exception as e:
        print(f"❌ 星表加载失败，请检查文件路径或格式: {e}")
        raise

    # 3. 计算坐标
    print("🔭 计算星体当前方位...")
    astrometric = earth.at(t).observe(stars)
    ra, dec, distance = astrometric.radec()

    # 4. 绘图渲染
    print("🎨 正在渲染 Canvas 背景图...")
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(16, 10), facecolor='#00050A') # 给画布上色
    ax.set_facecolor('#00050A') # 给绘图区上色 
    # 绘制星点
    ax.scatter(ra.hours, dec.degrees, s=1, color='#A0E6FF', edgecolors='none', alpha=0.3)
    
    # 标注名称
    star_names = ['Vega', 'Altair', 'Deneb']
    for i, name in enumerate(star_names):
        ax.text(ra.hours[i], dec.degrees[i] + 0.5, name, 
                color='white', fontsize=12, fontweight='bold', ha='center')

    # 美化：去除坐标轴，模拟深空感
    ax.axis('off')
    ax.set_facecolor('#00050A') # 深蓝黑底色
    
    # 5. 直接上传 S3 (内存流操作)
    img_buf = io.BytesIO()
    fig.savefig(img_buf, format='png', bbox_inches='tight', pad_inches=0, transparent=False, facecolor=fig.get_facecolor())
    img_buf.seek(0)
    plt.close(fig)

    print(f"☁️ 通过 {S3_CONN_ID} 上传至 S3: {S3_KEY}...")
    try:
        # 使用你确认的 Web Service 模式 Hook
        aws_hook = AwsGenericHook(aws_conn_id=S3_CONN_ID, client_type='s3')
        s3_client = aws_hook.get_conn()
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=img_buf,
            ContentType='image/png'
        )
        print("✅ 任务圆满完成！")
    except Exception as e:
        print(f"❌ S3 上传环节报错: {e}")
        raise

# 导入刚才我们写的渲染函数（假设你存为 render_logic.py，或者直接写在下面）
# from render_logic import render_celestial_background

default_args = {
    'owner': 'wpwang',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'astronomy_monitoring_render',
    default_args=default_args,
    description='渲染夏日大三角星图并上传至 S3 作为 Grafana 背景',
    schedule_interval='@hourly',  # 每小时更新一次星空相位
    catchup=False,
    tags=['sre', 'astronomy', 'v7_image'],
) as dag:

    render_task = PythonOperator(
        task_id='render_and_upload_star_chart',
        python_callable=render_celestial_background, # 调用上面那个函数
        # 通过 op_kwargs 注入环境变量，保持代码灵活性
        op_kwargs={
            'S3_BUCKET': 'wpwang-star',
            'S3_KEY': 'monitoring/summer_triangle_bg.png',
        },
        # 建议加上超时控制，防止读取大星表时卡死
        execution_timeout=timedelta(minutes=10),
    )

    render_task
