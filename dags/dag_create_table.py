from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task
import pendulum


@dag(
    dag_id="create_table_heroes",
    start_date=pendulum.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
)
def create_table():
    create_table_heroes: PostgresOperator = PostgresOperator(
        task_id="create_table_heroes",
        postgres_conn_id="stat_dota2",
        sql="sql/create_table/create_table_heroes.sql",
    )

    create_table_pro_teams = PostgresOperator(
        task_id="create_table_pro_teams",
        postgres_conn_id="stat_dota2",
        sql="sql/create_table/create_table_pro_teams.sql",
    )

    create_table_pro_players = PostgresOperator(
        task_id="create_table_pro_players",
        postgres_conn_id="stat_dota2",
        sql="sql/create_table/create_table_pro_players.sql",
    )

    create_table_pro_matches = PostgresOperator(
        task_id="create_table_pro_matches",
        postgres_conn_id="stat_dota2",
        sql="sql/create_table/create_table_pro_matches.sql",
    )

    create_table_heroes >> create_table_pro_teams >> create_table_pro_players >> create_table_pro_matches


dag = create_table()
