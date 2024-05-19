import os

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

dag = DAG(
    dag_id="create_tepm_tables",
    start_date=pendulum.datetime(2024, 5, 9),
    schedule="@once",
    catchup=True,
    tags=["table"],
    template_searchpath=f"{os.path.dirname(__file__)}/sql/create_temp_table",
)

create_table_items = SQLExecuteQueryOperator(
    task_id="create_temp_promatches_download_queue",
    conn_id="postgres",
    autocommit=True,
    database="postgres",
    sql="create_temp_promatches_download_queue.sql",
    dag=dag,
)




