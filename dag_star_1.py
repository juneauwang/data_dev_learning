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
HIP_FILE = 'hipparcos.gz'

S3_CONN_ID = 'aws_s3_conn'
S3_BUCKET = 'wpwang-star' 
S3_KEY = 'monitoring/summer_triangle_bg.png'

def render_celestial_background(**kwargs):
    """
    核心任务：加载本地 9000+ 颗星表，分层渲染深邃星空并上传 S3
    """
    # 1. 强制 Headless 模式
    plt.switch_backend('Agg')

    # 2. 加载本地数据
    print(f"🚀 正在从 {ASTRONOMY_DATA_DIR} 加载全量星表...")
    loader = SkyfieldLoader(ASTRONOMY_DATA_DIR)
    ts = loader.timescale()
    t = ts.now()

    planets = loader(BSP_FILE)
    earth = planets['earth']

    try:
        with loader.open(HIP_FILE) as f:
            df = hipparcos.load_dataframe(f)

        # ✨ 审美关键点 1：筛选 Vmag <= 6.5 的星星（约 9000 颗肉眼可见星）
        # 这样背景才不会只有“三盏灯”
        bright_stars_df = df[df['Vmag'] <= 6.5].copy()
        stars = Star.from_dataframe(bright_stars_df)
        print(f"✨ 成功加载 {len(bright_stars_df)} 颗星，准备点亮星空...")
    except Exception as e:
        print(f"❌ 星表加载失败: {e}")
        raise

    # 3. 计算坐标
    print("🔭 计算所有星体当前相位...")
    astrometric = earth.at(t).observe(stars)
    ra, dec, distance = astrometric.radec()

    # 4. 绘图渲染
    print("🎨 正在分层渲染深邃星空 Canvas...")
    plt.style.use('dark_background')
    
    # 设置画布，背景色设为深蓝色黑 #00050A
    fig, ax = plt.subplots(figsize=(16, 10), facecolor='#00050A')
    ax.set_facecolor('#00050A')

    # --- 第一层：绘制 9000 颗背景繁星 ---
    # s=1.2 让星星变细碎，alpha=0.4 让它们若隐若现
    ax.scatter(ra.hours, dec.degrees, s=1.2, color='white', edgecolors='none', alpha=0.4, zorder=1)

    # --- 第二层：精准抓取并高亮夏季大三角 ---
    # 织女星(91262), 牛郎星(97649), 天津四(102098)
    triangle_ids = [91262, 97649, 102098]
    star_names = ['Vega', 'Altair', 'Deneb']
    
    # 找到这三颗星在当前筛选结果中的位置索引
    try:
        indices = [bright_stars_df.index.get_loc(hip_id) for hip_id in triangle_ids]
        
        for idx, name in zip(indices, star_names):
            star_ra = ra.hours[idx]
            star_dec = dec.degrees[idx]

            # 画一个带光晕的大星点 (zorder=5 确保在背景之上)
            ax.scatter(star_ra, star_dec, s=120, color='#A0E6FF', 
                       edgecolors='white', linewidth=0.8, alpha=0.9, zorder=5)

            # 标注名称
            ax.text(star_ra, star_dec + 0.8, name, 
                    color='white', fontsize=14, fontweight='bold', 
                    ha='center', va='bottom', zorder=6)
    except Exception as e:
        print(f"⚠️ 标记大三角时出现小插曲（可能部分星等超限）: {e}")

    # --- 美化修正 ---
    # 审美关键点 2：翻转 X 轴，符合真实观星视角 (RA 向左增加)
    ax.invert_xaxis() 
    
    # 去除坐标轴
    ax.axis('off')

    # 5. 直接上传 S3 (内存流操作)
    img_buf = io.BytesIO()
    # 审美关键点 3：强制 transparent=False 并写入 facecolor，焊死黑色背景
    fig.savefig(img_buf, format='png', bbox_inches='tight', pad_inches=0.1, 
                transparent=False, facecolor=fig.get_facecolor())
    img_buf.seek(0)
    plt.close(fig)

    # 从传参中获取 Bucket 和 Key（如果没传则用默认值）
    bucket = kwargs.get('S3_BUCKET', S3_BUCKET)
    key = kwargs.get('S3_KEY', S3_KEY)

    print(f"☁️ 正在上传至 S3: {bucket}/{key}...")
    try:
        aws_hook = AwsGenericHook(aws_conn_id=S3_CONN_ID, client_type='s3')
        s3_client = aws_hook.get_conn()

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=img_buf,
            ContentType='image/png'
        )
        print("✅ 任务圆满完成！星空已点亮。")
    except Exception as e:
        print(f"❌ S3 上传环节报错: {e}")
        raise

# --- DAG 定义 ---
default_args = {
    'owner': 'wpwang',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'astronomy_monitoring_render',
    default_args=default_args,
    description='渲染分层繁星图并上传至 S3',
    schedule_interval='@hourly',
    catchup=False,
    tags=['sre', 'astronomy', 'v7_image'],
) as dag:

    render_task = PythonOperator(
        task_id='render_and_upload_star_chart',
        python_callable=render_celestial_background,
        op_kwargs={
            'S3_BUCKET': 'wpwang-star',
            'S3_KEY': 'monitoring/summer_triangle_bg.png',
        },
        execution_timeout=timedelta(minutes=15),
    )
