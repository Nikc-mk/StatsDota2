import os

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

dag = DAG(
    dag_id="create_tables",
    start_date=pendulum.datetime(2024, 5, 9),
    schedule="@once",
    catchup=True,
    tags=["upload"],
    template_searchpath=f"{os.path.dirname(__file__)}/sql/create_table",
)

create_table_items = SQLExecuteQueryOperator(
    task_id="create_table_items",
    conn_id="postgres",
    autocommit=True,
    database="postgres",
    sql="create_table_items.sql",
    dag=dag,
)

create_table_heroes = SQLExecuteQueryOperator(
    task_id="create_table_heroes",
    conn_id="postgres",
    autocommit=True,
    database="postgres",
    sql="create_table_heroes.sql",
    dag=dag,
)

create_table_pro_teams = SQLExecuteQueryOperator(
    task_id="create_table_pro_teams",
    conn_id="postgres",
    autocommit=True,
    database="postgres",
    sql="create_table_pro_teams.sql",
    dag=dag,
)

create_table_pro_matches = SQLExecuteQueryOperator(
    task_id="create_table_matches",
    conn_id="postgres",
    autocommit=True,
    database="postgres",
    sql="create_table_matches.sql",
    dag=dag,
)

create_table_pro_players = SQLExecuteQueryOperator(
    task_id="create_table_player_matches",
    conn_id="postgres",
    autocommit=True,
    database="postgres",
    sql="create_table_player_matches.sql",
    dag=dag,
)

(
    create_table_items
    >> create_table_heroes
    >> create_table_pro_teams
    >> create_table_pro_matches
    >> create_table_pro_players
)
