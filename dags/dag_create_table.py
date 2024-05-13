from airflow.decorators import dag
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="create_tables",
    start_date=pendulum.datetime(2024, 5, 9),
    schedule="@once",
    catchup=True,
    tags=["upload"],
)
def create_table():
    """
    Создает таблицы в базе данных PostgreSQL для хранения информации о предметах, героях,
    профессиональных командах и матчах.

    Выполняет последовательное создание таблиц с использованием оператора SQLExecuteQueryOperator.
    Созданные таблицы: items, heroes, pro_teams, matches, player_matches.

    Возвращает:
    None
    """
    create_table_items: operator = SQLExecuteQueryOperator(
        task_id="create_table_items",
        conn_id="postgres",
        autocommit=True,
        database="postgres",
        sql="sql/create_table/create_table_items.sql"
    )

    create_table_heroes: operator = SQLExecuteQueryOperator(
        task_id="create_table_heroes",
        conn_id="postgres",
        autocommit=True,
        database="postgres",
        sql="sql/create_table/create_table_heroes.sql",
    )

    create_table_pro_teams: operator = SQLExecuteQueryOperator(
        task_id="create_table_pro_teams",
        conn_id="postgres",
        autocommit=True,
        database="postgres",
        sql="sql/create_table/create_table_pro_teams.sql",
    )

    create_table_pro_matches: operator = SQLExecuteQueryOperator(
        task_id="create_table_matches",
        conn_id="postgres",
        autocommit=True,
        database="postgres",
        sql="sql/create_table/create_table_matches.sql",
    )

    create_table_pro_players: operator = SQLExecuteQueryOperator(
        task_id="create_table_player_matches",
        conn_id="postgres",
        autocommit=True,
        database="postgres",
        sql="sql/create_table/create_table_player_matches.sql",
    )

    (create_table_items >> create_table_heroes >> create_table_pro_teams >> create_table_pro_matches >>
     create_table_pro_players)


dag = create_table()
