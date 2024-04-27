from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag
import pendulum


@dag(
    default_args={
        "depends_on_past": False,
        "email": ["email@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    dag_id="create_tables",
    start_date=pendulum.datetime(2024, 4, 27),
    schedule="@once",
    catchup=False,
)
def create_table():
    """
    Эта функция создает четыре таблицы в базе данных Postgres: heroes, pro_teams, pro_players, и pro_matches.
    Код организован как диаграмма связей (DAG) с использованием библиотеки Airflow.
    Параметр default_args определяет дефолтные аргументы для всех задач в диаграмме, включая уведомления электронной
    почты и политики повторных попыток.
    Класс PostgresOperator используется для выполнения SQL-запросов в базе данных.
    Параметр schedule указывает, что диаграмма должна быть выполнена один раз, на указанную дату start_date.
    Параметр catchup установлен на False, чтобы предотвратить выполнение диаграммы за пределами расписания.
    """
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
