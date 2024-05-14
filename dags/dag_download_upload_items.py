import pendulum
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 5, 10, tz="UTC"),
    catchup=True,
    tags=["upload"],
)
def dag_download_upload_items():
    """
    Эта DAG загружает данные о предметах из API OpenDota в базу данных PostgreSQL.
    Она выполняется один раз и запускается по расписанию "@once".
    """

    @task()
    def api_get_heroes():
        """
        Эта функция выполняет HTTP-запрос к OpenDota и возвращает данные о предметах в формате JSON.
        :return: Список словарей, каждый из которых представляет один предмет.
        """
        hook = HttpHook(method="GET", http_conn_id="opendota")
        response = hook.run(endpoint="/api/explorer?sql=SELECT%20*%20from%20items")
        items = response.json()
        print(f"Извлечено {len(items)} предметов.")

        return items["rows"]

    @task
    def check_count_items(items: list):
        """
        Эта функция проверяет, есть ли в API новые предметы, которые не существуют в базе данных PostgreSQL.
        Если такие предметы есть, то вся таблица items очищается.
        :param items: Список словарей, каждый из которых представляет один предмет.
        :return: Список словарей, если есть новые предметы, или False, если все предметы уже есть в базе.
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        con_pg_hook = pg_hook.get_conn()
        cur_pg_hook = con_pg_hook.cursor()
        query = """
        SELECT COUNT(*) FROM HEROES
        """
        cur_pg_hook.execute(query)
        count = cur_pg_hook.fetchone()[0]
        if count == len(items):
            print("Все предметы уже есть в базе данных.")
            items = False
        else:
            print(f"Есть {len(items) - count} предметов для загрузки.")
            query_del_heroes = """
                    TRUNCATE ITEMS
                    """
            cur_pg_hook.execute(query_del_heroes)
        con_pg_hook.close()
        return items

    @task(multiple_outputs=True)
    def upload_data_items(data_items: list | bool):
        """
        Эта функция загружает предметы в базу данных PostgreSQL.
        :param data_items: Список словарей, каждый из которых представляет один предмет, или False,
         если все предметы уже есть в базе.
        """
        if data_items is False:
            return print("ВСЕ предметы ЗАГРУЖЕНЫ")

        for item in data_items:
            try:
                hook = PostgresHook(postgres_conn_id="postgres")
                hook.insert_rows(
                    table="items",
                    rows=[
                        (
                            item["id"],
                            item["name"],
                            item["cost"],
                            item["secret_shop"],
                            item["side_shop"],
                            item["recipe"],
                            item["localized_name"],
                        )
                    ],
                    target_fields=[
                        "id",
                        "name",
                        "cost",
                        "secret_shop",
                        "side_shop",
                        "recipe",
                        "localized_name",
                    ],
                )
            except Exception as e:
                print(e)

    data_item = api_get_heroes()
    data_item = check_count_items(data_item)
    upload_data_items(data_item)


dag_download_upload_items()
