from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import random

# 1. 定义 Python 逻辑：模拟处理数据
def process_data_logic(**kwargs):
    # ti (task_instance) 用于在任务间传递小量数据 (XComs)
    input_value = kwargs['ti'].xcom_pull(task_ids='download_data')
    print(f"正在处理来自上游的数据: {input_value}")
    
    if random.random() > 0.5:
        return "数据处理成功！"
    else:
        raise Exception("模拟数据校验失败")

# 2. 定义 DAG 结构
with DAG(
    dag_id='learning_complex_dag_v1',
    default_args={'owner': 'juneauwang'},
    description='一个包含 Bash 和 Python 依赖的复杂 DAG',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['learning', 'git_sync'],
) as dag:

    # 任务 A: BashOperator (模拟下载)
    t1 = BashOperator(
        task_id='download_data',
        bash_command='echo "START_DATALAKE_$(date +%Y%m%d)"',
    )

    # 任务 B: PythonOperator (模拟业务逻辑)
    t2 = PythonOperator(
        task_id='process_data',
        python_callable=process_data_logic,
    )

    # 任务 C: BashOperator (模拟清理或通知)
    t3 = BashOperator(
        task_id='notify_success',
        bash_command='echo "通知：数据已成功归档并同步到 GitHub。"',
    )

    # 3. 设置依赖关系 (依赖链)
    t1 >> t2 >> t3
