from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator # 1.10.x 可能是 DummyOperator
from airflow.utils.dates import days_ago
import random

def check_data_availability():
    # 模拟检查：50% 概率有数据
    has_data = random.choice([True, False])
    print(f"数据检查结果: {'有数据' if has_data else '无数据'}")
    
    if has_data:
        return 'run_pandas_etl' # 返回下一个要运行的 task_id
    else:
        return 'skip_notification'

def etl_logic():
    rows_processed = 100 # 模拟处理了 100 行
    # 推送到 XCom
    context['ti'].xcom_push(key='rows_count', value=rows_processed)
    print(f"🚀 ETL 完成，处理了 {rows_processed} 行。")
    #print("🚀 正在执行复杂的 ETL 逻辑...")

def notify_logic():
    print("📢 通知：今日无新数据，流程结束。")

def cleanup_logic():
    # 尝试从上游获取行数
    rows = context['ti'].xcom_pull(task_ids='run_pandas_etl', key='rows_count')
    
    if rows:
        print(f"🧹 任务汇总：今日处理了 {rows} 条数据，清理完毕。")
    else:
        print("🧹 任务汇总：今日无数据处理，环境已重置。")
    print ("🧹 正在清理环境并发送最终报告...")
with DAG(
    'smart_branching_workflow_v1',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    # 分支节点
    branching = BranchPythonOperator(
        task_id='branching_node',
        python_callable=check_data_availability
    )

    # 路径 A
    run_etl = PythonOperator(
        task_id='run_pandas_etl',
        python_callable=etl_logic
    )

    # 路径 B
    skip_notify = PythonOperator(
        task_id='skip_notification',
        python_callable=notify_logic
    )
    final_task = PythonOperator(
    	task_id='final_operator',
	python_callable=cleanup_logic,
	trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS



    )

    # 定义依赖关系
    branching >> [run_etl, skip_notify] >> final_task
