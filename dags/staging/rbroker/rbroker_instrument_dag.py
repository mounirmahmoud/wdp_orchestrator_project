import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "rbroker", "tags" : "rbroker", "start_date": datetime(2021,3,22,17,15)}

query1 = [
    """select 1;""",
    """show tables in database wdp_staging;""",
]

def count1(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    result = dwh_hook.get_first("select count(*) from wdp_staging.staging.my_first_dbt_model")
    logging.info("Number of rows in `wdp_staging.staging.my_first_dbt_model`  - %s", result[0])


dag = DAG(
    dag_id="rbroker_instrument_dag", default_args=args, schedule_interval=None
)

with dag:
    query1_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql=query1,
        snowflake_conn_id="my_snowflake_conn",
    )

    count_query1 = PythonOperator(task_id="count_query1", python_callable=count1)

    count_query2 = PythonOperator(task_id="count_query2", python_callable=count1)

    count_query3 = PythonOperator(task_id="count_query3", python_callable=count1)

query1_exec >> count_query1 >> count_query2 >> count_query3