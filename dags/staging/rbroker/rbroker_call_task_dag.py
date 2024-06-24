from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'rbroker_call_tasks_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['rbroker'],
)

start_task_A = SnowflakeOperator(
    task_id='start_task_A',
    sql='CALL system$task_run("TASK_1");',
    snowflake_conn_id='wedp_snowflake_con',
    dag=dag,
)

sensor_task_A = SqlSensor(
    task_id='sensor_task_A',
    conn_id='wedp_snowflake_con',
    sql="SELECT * FROM TABLE(information_schema.task_history) WHERE NAME='TASK_1' AND STATE='SUCCESS';",
    dag=dag,
)

start_task_B = SnowflakeOperator(
    task_id='start_task_B',
    sql='CALL system$task_run("TASK_2");',
    snowflake_conn_id='wedp_snowflake_con',
    dag=dag,
)

start_task_C = SnowflakeOperator(
    task_id='start_task_C',
    sql='CALL system$task_run("TASK_2");',
    snowflake_conn_id='wedp_snowflake_con',
    dag=dag,
)

start_task_D = SnowflakeOperator(
    task_id='start_task_D',
    sql='CALL system$task_run("TASK_2");',
    snowflake_conn_id='wedp_snowflake_con',
    dag=dag,
)

start_task_A >> sensor_task_A >> start_task_B >> [start_task_C, start_task_D]