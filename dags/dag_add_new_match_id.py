import pendulum
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 5, 10, tz="UTC"),
    catchup=False,
    tags=["upload"],
)
def dag_add_new_match_id():
    """
    Эта DAG загружает идентификаторы профессиональных матчей из API OpenDota во временную таблицу Postgres.
    Запускается ежедневно.
    """

    @task()
    def api_get_pro_match_id():
        """
        Запрашивает список профессиональных матчей с API OpenDota.
        Возвращает список профессиональных матчей.
        """
        hook = HttpHook(method="GET", http_conn_id="opendota")
        try:
            response = hook.run(endpoint="/api/proMatches")
            response.raise_for_status()  # Проверка на успешный статус ответа
            pro_matches = response.json()
            print(f"Загружено {len(pro_matches)} профессиональных матчей.")
        except Exception as e:
            raise AirflowException(f"Ошибка при запросе к API OpenDota: {str(e)}")

        return pro_matches

    @task()
    def upload_match_id_to_temp_table(pro_matches: list):
        """
        Записывает полученные идентификаторы матчей во временную таблицу Postgres.
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        try:
            # Записываем match_id
            for pro_match in pro_matches:
                pg_hook.insert_rows(
                    table="temp_promatches_download_queue",
                    replace=True,
                    replace_index="match_id",
                    rows=[(pro_match["match_id"],)],
                    target_fields=["match_id"],
                )
            print(f"Успешно записано {len(pro_matches)} матчей в таблицу.")
        except Exception as e:
            raise AirflowException(f"Ошибка при записи в Postgres: {str(e)}")

    matches = api_get_pro_match_id()
    upload_match_id_to_temp_table(matches)

dag_add_new_match_id()