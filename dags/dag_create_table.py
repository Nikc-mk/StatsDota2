from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag
import pendulum


@dag(
    dag_id="create_tables",
    start_date=pendulum.datetime(2024, 4, 27),
    schedule="@once",
    catchup=False,
    tags=["upload"],
)
def create_table():
    """
    Создает таблицы в базе данных PostgreSQL для хранения информации о предметах, героях,
    профессиональных командах и матчах.

    Выполняет последовательное создание таблиц с использованием оператора PostgresOperator.
    Созданные таблицы: items, heroes, pro_teams, matches, player_matches.

    Возвращает:
    None
    """
    create_table_items: PostgresOperator = PostgresOperator(
        task_id="create_table_items",
        postgres_conn_id="postgres",
        sql="sql/create_table/create_table_items.sql",
    )

    create_table_heroes: PostgresOperator = PostgresOperator(
        task_id="create_table_heroes",
        postgres_conn_id="postgres",
        sql="sql/create_table/create_table_heroes.sql",
    )

    create_table_pro_teams = PostgresOperator(
        task_id="create_table_pro_teams",
        postgres_conn_id="postgres",
        sql="sql/create_table/create_table_pro_teams.sql",
    )

    create_table_pro_matches = PostgresOperator(
        task_id="create_table_matches",
        postgres_conn_id="postgres",
        sql="sql/create_table/create_table_matches.sql",
    )

    create_table_pro_players = PostgresOperator(
        task_id="create_table_player_matches",
        postgres_conn_id="postgres",
        sql="sql/create_table/create_table_player_matches.sql",
    )

    (create_table_items >> create_table_heroes >> create_table_pro_teams >> create_table_pro_matches >>
     create_table_pro_players)


dag = create_table()
