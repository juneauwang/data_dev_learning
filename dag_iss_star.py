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
        stars = hipparcos.load_dataframe(f)

    # C. 绘图设置
    fig = plt.figure(figsize=(10, 10), facecolor='#000008')
    ax = fig.add_subplot(111, projection='polar', facecolor='#000008')
    bright_stars = stars[stars['magnitude'] <= 5.5]    
    # 极坐标转换逻辑
    # 1. 绘制背景星空
    ra_rad = bright_stars.ra_hours * (np.pi / 12.0)
    r_val = 90 - bright_stars['dec_degrees']
    
    # 筛选 FOV 范围内的星 (简化版)
    ax.scatter(ra_rad, r_val, s=(6.0 - bright_stars['magnitude'])**2 * 0.5, 
               color='white', alpha=0.9, edgecolors='none',zorder=1)
    DYNAMIC_CENTER_RA = 19.5
    DYNAMIC_CENTER_DEC = 30.0

    # 2. 绘制 ISS 轨迹
    if df is not None and not df.empty:
        print(f"✅ 捕获到 {len(df)} 条实时轨迹点")
        # 将 LLA 转为 RA/Dec (简化地心投影)
        # SRE 面试重点: 这里体现了跨域数据融合能力
        iss_times = ts_scale.from_datetimes(df['ts'].dt.tz_localize('UTC'))
        iss_ra = []
        iss_dec = []
        # 1. 获取 ISS 最新的赤经赤纬 (RA/Dec)
        # 我们直接用最后一点的坐标
        for i in range(len(df)):
            p = wgs84.latlon(df.iloc[i]['lat'], df.iloc[i]['lon'])
            astrometric = earth.at(iss_times[i]).observe(p)
            ra, dec, _ = astrometric.radec()
            iss_ra.append(ra.hours * (np.pi / 12.0))
            iss_dec.append(90 - dec.degrees)
        if len(df) > 1:
        # 绘制亮橘色轨迹
           ax.plot(iss_ra, iss_dec, color='#FF4500', linewidth=5.0, alpha=1.0,linestyle='-', label='ISS Tracker',zorder=20)
        # 标注当前点
           ax.scatter(iss_ra[-1], iss_dec[-1], color='#FFD700', s=350, marker='h', label='Live ISS',edgecolors='white', linewidths=2.0, zorder=21)
           last_point = df.iloc[-1]
           ax.text(iss_ra[-1], iss_dec[-1] + 1, # 在 ISS 上方偏一点标注
                f"Live ISS Now\n({last_point['lat']:.2f}, {last_point['lon']:.2f})", 
                color='#FFD700', fontsize=12, fontweight='bold', 
                ha='center', va='bottom', zorder=30) # zorder=30i
        DYNAMIC_CENTER_RA = ra.hours # 使用最后一次循环的 RA (小时)
        DYNAMIC_CENTER_DEC = dec.degrees # 使用最后一次循环的 Dec (角度)
        print(f"🎯 自动对焦至 ISS 位置: RA {DYNAMIC_CENTER_RA:.2f}, Dec {DYNAMIC_CENTER_DEC:.2f}")

        
    # 3. 极坐标美化
    # --- 4. 极坐标美化 (SRE 强制对焦版) ---
    ax.set_theta_zero_location('N')
    ax.set_theta_direction(-1) 
    
    # 计算当前 ISS 的 R 值 (离北极的距离)
    # ISS Lat 3.35 -> R = 90 - 3.35 = 86.65
    current_iss_r = 90 - DYNAMIC_CENTER_DEC
    
    # 强制将 R 轴中心锁定在 ISS 附近，FOV 设为 40 度
    # 这样不管它在赤道还是南极，镜头都会跟过去
    ax.set_rlim(current_iss_r + 20, current_iss_r - 20) 

    # 彻底移除角度限制，防止扇形切割
    ax.set_xlim(0, 2*np.pi) 

    if df is not None and not df.empty:
        # 画一个超级巨大的红点，zorder 拉到最高
        ax.scatter(iss_ra[-1], iss_dec[-1], color='#FF0000', s=2000, 
                   marker='o', edgecolors='white', linewidths=4, zorder=500)
        
        # 强制标注
        ax.text(iss_ra[-1], iss_dec[-1], " <--- ISS HERE", 
                color='red', fontsize=25, fontweight='bold', zorder=501)

    ax.grid(True, color='gray', alpha=0.3) # 暂时开启网格，方便肉眼定位坐标
    # 强制刷新图例，确保亮橘色和金色六角星的说明出现在右上角
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
