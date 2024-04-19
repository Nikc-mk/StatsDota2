from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow import DAG

with DAG(
    dag_id="create_pet_table_in_pg",
    start_date=datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    task_create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="pg_test",
        sql="sql/pet_schema.sql",
    )


