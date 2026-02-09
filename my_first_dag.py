from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='vscode_dev_test',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # 手動觸發
    tags=['learning']
) as dag:

    hello_task = BashOperator(
        task_id='hello_world',
        bash_command='echo "Hello from VS Code and K8S!"'
    )

    