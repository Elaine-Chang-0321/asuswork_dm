from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import os

with DAG(
    "CSGP_local_excel_import",
    default_args={
        'owner': 'Lexi_Lu',
        'depends_on_past': False,
        'start_date': datetime(2025, 12, 19),
        'email': ['lexi_lu@asus.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='將FTP內的檔案匯入到MSSQL APZA002BID的CSGP資料庫',
    schedule_interval='00 20 * * *',
    catchup=True,
    tags=['CSGP']
) as dag:
    run_main = BashOperator(
        task_id='run_excel_import',
        bash_command=f'cd "{os.path.dirname(__file__)}" && python main.py',
    )

    run_main