# from typing import List

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

# import pydantic

# 
# class Hero(pydantic.BaseModel):
#     id: int
#     name: str
#     localized_name: str
#     primary_attr: str
#     attack_type: str
#     roles: List[str]
#     legs: int


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 5, 10, tz="UTC"),
    catchup=True,
    tags=["upload"],
)
def dag_download_upload_heroes():
    """
    Эта функция создает pipline Airflow, который загружает данные о героях в базу данных PostgreSQL.
    Для этого она использует поставляемые с Airflow инструменты, такие как PostgresHook и insert_rows.
    Вначале она вызывает api_get_heroes, которая выполняет HTTP-запрос к OpenDota и возвращает данные
    о героях в формате JSON.
    Затем она вызывает check_count_heroes, которая выполняет запрос к базе данных PostgreSQL, чтобы проверить,
    есть ли уже все герои.
    Если все герои есть, она возвращает False и завершает pipline.
    Если нет, она выводит сообщение о том, сколько героев нужно загрузить, а затем выполняет запрос,
    чтобы удалить все герои из базы данных.
    Потом вызывает upload_data_heroes, которая перебирает полученные данные о героях и вызывает insert_rows
    для каждого героя, чтобы загрузить его в базу данных.
    В конце она выводит сообщение о том, что все герои были загружены.
    """

    @task()
    def api_get_heroes():
        """
        Эта функция выполняет HTTP-запрос к OpenDota и возвращает данные о героях в формате JSON.
        :return:
        """
        hook = HttpHook(method='GET', http_conn_id='opendota')
        response = hook.run(endpoint="/api/heroes")
        heroes = response.json()
        print(f"Fetched {len(heroes)} heroes.")

        return heroes

    @task
    def check_count_heroes(heroes: list):
        """
        Эта функция выполняет запрос к базе данных PostgreSQL, чтобы проверить, есть ли уже все герои.
        Если все герои есть, она возвращает False и завершает pipline.
        Если нет, она выводит сообщение о том, сколько героев нужно загрузить, а затем выполняет запрос,
        чтобы удалить все герои из базы данных.
        :param heroes:
        :return:
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        con_pg_hook = pg_hook.get_conn()
        cur_pg_hook = con_pg_hook.cursor()
        query = """
        SELECT COUNT(*) FROM HEROES
        """
        cur_pg_hook.execute(query)
        count = cur_pg_hook.fetchone()[0]
        if count == len(heroes):
            print("All heroes are already in the database.")
            heroes = False
        else:
            print(f"There are {len(heroes) - count} heroes to be uploaded.")
            query_del_heroes = """
                    TRUNCATE HEROES
                    """
            cur_pg_hook.execute(query_del_heroes)
        con_pg_hook.close()
        return heroes

    @task(multiple_outputs=True)
    def upload_data_heroes(heroes: list | bool):
        """
        Эта функция перебирает полученные данные о героях и вызывает insert_rows для каждого героя,
        чтобы загрузить его в базу данных.
        :param heroes:
        :return:
        """
        if heroes is False:
            return print("ALL heroes are UPLOAD")

        for hero in heroes:
            try:
                src = PostgresHook(postgres_conn_id="postgres")
                src.insert_rows(table="heroes",
                                rows=[(hero["id"], hero["name"], hero["localized_name"], hero["primary_attr"],
                                       hero["attack_type"], hero["roles"],)],
                                target_fields=["id", "name", "localized_name", "primary_attr", "attack_type", "roles"])
            except Exception as e:
                print(e)

    data_heroes = api_get_heroes()
    data_heroes = check_count_heroes(data_heroes)
    upload_data_heroes(data_heroes)


dag_download_upload_heroes()
