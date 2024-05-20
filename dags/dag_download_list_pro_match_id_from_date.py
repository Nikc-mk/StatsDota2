from datetime import timedelta

import pendulum
import tenacity
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATE_START = "2024-04-01"

"""
Определяет DAG для загрузки списка идентификаторов профессиональных матчей, начиная с определенной даты.

Функции:
- api_get_list_pro_match_id_from_date: Вызывает API OpenDota для получения списка идентификаторов профессиональных матчей,
  начиная с даты DATE_START.
- upload_match_id_to_temp_table: Записывает полученные идентификаторы матчей во временную таблицу Postgres.
"""


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2024, 5, 10, tz="UTC"),
    catchup=True,
    tags=["upload"],
)
def dag_download_list_pro_match_id_from_date():
    @task()
    def api_get_list_pro_match_id_from_date():
        """
        Вызывает API OpenDota для получения списка идентификаторов профессиональных матчей,
        начиная с даты DATE_START.
        """
        hook = HttpHook(method="GET", http_conn_id="opendota")
        response = hook.run(
            endpoint="/api/explorer?sql=SELECT match_id FROM matches join leagues using(leagueid)"
            "WHERE TRUE AND "
            f"matches.start_time >= extract(epoch from timestamp '{DATE_START}')"
            "AND leagues.tier = 'professional'"
        )
        pro_matches = response.json()
        print(f"ИЗВЛЕЧЕНО {len(pro_matches["rows"])} ПРО МАТЧЕЙ.")

        return pro_matches["rows"]

    @task()
    def upload_match_id_to_temp_table(pro_matches: list):
        """
        Записывает полученные идентификаторы матчей во временную таблицу Postgres.
        """
        # Записываем match_id
        for pro_match in pro_matches:
            pg_hook = PostgresHook(postgres_conn_id="postgres")
            pg_hook.insert_rows(
                table="temp_promatches_download_queue",
                replace=True,
                replace_index="match_id",
                rows=[(pro_match["match_id"],)],
                target_fields=["match_id"],
            )

    matches = api_get_list_pro_match_id_from_date()
    upload_match_id_to_temp_table(matches)


dag_download_list_pro_match_id_from_date()
