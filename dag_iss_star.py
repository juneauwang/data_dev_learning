from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import requests
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from skyfield.api import load, wgs84
from skyfield.data import hipparcos
import io
import os
from skyfield.api import Loader as SkyfieldLoader
# --- 1. 配置参数 (SRE 生产环境建议) ---
# K8S Service 内部地址
CH_URL = "http://clickhouse-headless:8123/" 
SAVE_PATH = "/tmp/iss_star_monitoring.png"  # 临时存放，后续可接 S3 上传

# 星图中心: 夏季大三角 (RA: 19.5h, Dec: 30°)
CENTER_RA = 19.5
CENTER_DEC = 30.0
FOV = 80.0

# --- 2. 核心逻辑函数 ---

def get_ch_data(hours=1):
    """使用 HTTP 协议从 ClickHouse 捞取实时清洗后的 ISS 数据"""
    query = f"""
    SELECT ts, lat, lon 
    FROM default.iss_clean 
    WHERE ts > now() - interval {hours} hour 
    ORDER BY ts ASC 
    FORMAT JSONEachRow
    """
    try:
        # SRE 技巧: 使用 POST 避免长 SQL 在 URL 中截断
        response = requests.post(CH_URL, data=query, timeout=15)
        if response.status_code == 200 and response.text.strip():
            # 解析 JSONEachRow 格式
            lines = response.text.strip().split('\n')
            data = [json.loads(line) for line in lines]
            df = pd.DataFrame(data)
            df['ts'] = pd.to_datetime(df['ts'])
            return df
        return None
    except Exception as e:
        print(f"🚨 ClickHouse 连接失败: {e}")
        return None

def render_astronomy_monitoring():
    """主渲染逻辑: 银河背景 + ISS 轨迹"""
    print("🌌 开始渲染星空监控图...")
    
    # A. 获取数据
    df = get_ch_data(hours=1)

    load_local = SkyfieldLoader('/home/airflow/astronomy_data')    
    # B. 加载历表 (Skyfield 会自动缓存到本地)
    eph = load_local('de421.bsp')
    earth = eph['earth']
    ts_scale = load.timescale()
    
    # 加载 HIP 星表 (需确保 Worker 能访问网络或预置文件)
    with load_local.open('hipparcos.gz') as f:
        stars = hipparcos.load_hipparcos(f)

    # C. 绘图设置
    fig = plt.figure(figsize=(10, 10), facecolor='#000008')
    ax = fig.add_subplot(111, projection='polar', facecolor='#000008')
    
    # 极坐标转换逻辑
    # 1. 绘制背景星空
    ra_rad = stars['ra_hours'] * (np.pi / 12.0)
    r_val = 90 - stars['dec_degrees']
    
    # 筛选 FOV 范围内的星 (简化版)
    ax.scatter(ra_rad, r_val, s=(6.0 - stars['magnitude'])**2 * 0.3, 
               color='white', alpha=0.8, edgecolors='none')

    # 2. 绘制 ISS 轨迹
    if df is not None and not df.empty:
        print(f"✅ 捕获到 {len(df)} 条实时轨迹点")
        # 将 LLA 转为 RA/Dec (简化地心投影)
        # SRE 面试重点: 这里体现了跨域数据融合能力
        iss_times = ts_scale.from_datetimes(df['ts'].dt.tz_localize('UTC'))
        iss_ra = []
        iss_dec = []
        
        for i in range(len(df)):
            p = wgs84.latlon(df.iloc[i]['lat'], df.iloc[i]['lon'])
            astrometric = earth.at(iss_times[i]).observe(p)
            ra, dec, _ = astrometric.radec()
            iss_ra.append(ra.hours * (np.pi / 12.0))
            iss_dec.append(90 - dec.degrees)

        # 绘制亮橘色轨迹
        ax.plot(iss_ra, iss_dec, color='#FF4500', linewidth=2, alpha=0.9, label='ISS Tracker')
        # 标注当前点
        ax.scatter(iss_ra[-1], iss_dec[-1], color='#FFD700', s=100, marker='h', label='Live ISS')
        
    # 3. 极坐标美化
    ax.set_theta_zero_location('N')
    ax.set_theta_direction(-1)
    ax.set_rlim(90 - (CENTER_DEC + FOV/2), 90 - (CENTER_DEC - FOV/2))
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.grid(False)
    
    plt.title(f"Live ISS Monitoring | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}", 
              color='white', pad=20)
    
    # D. 保存到本地 (之后可以增加 S3 上传代码)
    plt.savefig(SAVE_PATH, facecolor=fig.get_facecolor(), bbox_inches='tight')
    print(f"🎯 监控图已生成: {SAVE_PATH}")

# --- 3. Airflow DAG 定义 ---

with DAG(
    'iss_astronomy_render_v2',
    default_args={'owner': 'wpwang'},
    description='渲染银河背景下的 ISS 实时轨迹监控图',
    schedule_interval='*/10 * * * *',  # 每10分钟更新一次图
    start_date=datetime.datetime(2026, 3, 28),
    catchup=False,
    tags=['SRE', 'Visualization', 'ClickHouse']
) as dag:

    render_task = PythonOperator(
        task_id='render_iss_star_map',
        python_callable=render_astronomy_monitoring
    )
