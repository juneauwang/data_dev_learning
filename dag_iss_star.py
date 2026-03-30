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
from skyfield.api import Loader as SkyfieldLoader
import os

# --- 1. 配置参数 ---
CH_URL = "http://clickhouse-headless:8123/"
SAVE_PATH = "/tmp/iss_star_monitoring.png"
DATA_DIR = '/home/airflow/astronomy_data'

def get_ch_data(hours=1):
    """从 ClickHouse 捞取数据并强制转型"""
    query = f"SELECT ts, lat, lon FROM default.iss_final WHERE ts > now() - interval {hours} hour ORDER BY ts ASC FORMAT JSONEachRow"
    try:
        response = requests.post(CH_URL, data=query, timeout=15)
        if response.status_code == 200 and response.text.strip():
            lines = response.text.strip().split('\n')
            data = [json.loads(line) for line in lines]
            df = pd.DataFrame(data)
            # SRE 加固：强制转换为数值，防止 ClickHouse 吐出字符串导致绘图失败
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
            df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
            df['ts'] = pd.to_datetime(df['ts'])
            return df.dropna(subset=['lat', 'lon'])
        return None
    except Exception as e:
        print(f"🚨 ClickHouse 连接失败: {e}")
        return None

def render_astronomy_monitoring():
    print("🌌 开始执行 SRE 级全量对焦渲染...")
    df = get_ch_data(hours=1)
    if df is None or df.empty:
        print("⚠️ 1小时内无数据，尝试回溯24小时...")
        df = get_ch_data(hours=24) # 只要库里有数据，就一定能画出来   
    if df is None or df.empty:
        # 如果还是没有，构造一个伪数据点进行“链路压测”
        print("🚨 库里彻底没数据了！构造测试点...")
        df = pd.DataFrame([{'ts': datetime.datetime.now(), 'lat': 3.3, 'lon': -110.0}])
    # ---------------------------- 
    # 加载星历
    load_local = SkyfieldLoader(DATA_DIR)
    eph = load_local('de421.bsp')
    earth = eph['earth']
    ts_scale = load.timescale()

    with load_local.open('hipparcos.gz') as f:
        stars = hipparcos.load_dataframe(f)

    # 绘图初始化
    fig = plt.figure(figsize=(12, 12), facecolor='#000008')
    ax = fig.add_subplot(111, projection='polar', facecolor='#000008')
    
    # 1. 绘制背景亮星
    bright_stars = stars[stars['magnitude'] <= 5.8]
    ra_rad = bright_stars.ra_hours * (np.pi / 12.0)
    r_val = 90 - bright_stars['dec_degrees']
    ax.scatter(ra_rad, r_val, s=(6.0 - bright_stars['magnitude'])**2 * 0.4,
               color='white', alpha=0.6, edgecolors='none', zorder=1)

    # 2. 核心逻辑：渲染 ISS 轨迹与对焦
    if df is not None and not df.empty:
        # 转换 ISS 坐标
        iss_times = ts_scale.from_datetimes(df['ts'].dt.tz_localize('UTC'))
        iss_ra, iss_dec_r = [], []
        
        for i in range(len(df)):
            # 直接计算 ISS 在地心坐标系下的位置
            p = earth + wgs84.latlon(df.iloc[i]['lat'], df.iloc[i]['lon'])
            # position_at 会返回一个相对于地心的位置，不再需要 SSB
            astrometric = p.at(iss_times[i])
            ra, dec, _ = astrometric.radec()
            
            iss_ra.append(ra.hours * (np.pi / 12.0))
            iss_dec_r.append(90 - dec.degrees)

        # 绘制实时轨迹 (亮橘色)
        ax.plot(iss_ra, iss_dec_r, color='#FF4500', linewidth=4.0, alpha=0.8, zorder=20)
        
        # 绘制 ISS 当前点 (巨大红色五角星)
        last_ra = iss_ra[-1]
        last_r = iss_dec_r[-1]
        ax.scatter(last_ra, last_r, color='red', s=1500, marker='*', 
                   edgecolors='white', linewidths=2, zorder=100)
        
        # 强制文字标注
        ax.text(last_ra, last_r + 5, f"🎯 TARGET ISS\nLat:{df.iloc[-1]['lat']:.1f}", 
                color='red', fontsize=18, fontweight='bold', ha='center', zorder=101)

        # --- 3. 动态对焦设置 ---
        ax.set_theta_zero_location('N')
        ax.set_theta_direction(-1) 
        # 锁定 ISS 所在的纬度带，前后扩充 30 度
        ax.set_rlim(last_r + 30, last_r - 30) 
        # 全圆显示，不设限
        ax.set_xlim(0, 2*np.pi) 
    else:
        # 无数据时的默认视角
        ax.set_rlim(180, 0)

    # 美化去除标签
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.grid(True, color='white', alpha=0.1) # 淡淡的网格辅助对焦
    
    plt.title(f"Live ISS Tracking | {datetime.datetime.now().strftime('%H:%M')}", 
              color='white', pad=30, fontsize=15)

    plt.savefig(SAVE_PATH, facecolor=fig.get_facecolor(), bbox_inches='tight')
    print(f"✅ 渲染成功: {SAVE_PATH}")

# --- 4. Airflow DAG ---
with DAG(
    'iss_astronomy_render_final',
    default_args={'owner': 'wpwang'},
    schedule_interval='*/10 * * * *',
    start_date=datetime.datetime(2026, 3, 29),
    catchup=False,
    tags=['SRE', 'Production']
) as dag:
    PythonOperator(
        task_id='render_iss_map',
        python_callable=render_astronomy_monitoring
    )
