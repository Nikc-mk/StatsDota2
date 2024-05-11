import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2024, 4, 26, tz="UTC"),
    catchup=False,
    tags=["upload"],
)
def dag_download_upload_items():
    @task()
    def api_get_heroes():
        """
        Эта функция выполняет HTTP-запрос к OpenDota и возвращает данные о героях в формате JSON.
        :return:
        """
        hook = HttpHook(method='GET', http_conn_id='opendota')
        response = hook.run(endpoint="/api/explorer?sql=SELECT%20*%20from%20items")
        items = response.json()
        print(f"Fetched {len(items)} items.")

        return items["rows"]

    @task
    def check_count_items(items: list):
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        con_pg_hook = pg_hook.get_conn()
        cur_pg_hook = con_pg_hook.cursor()
        query = """
        SELECT COUNT(*) FROM HEROES
        """
        cur_pg_hook.execute(query)
        count = cur_pg_hook.fetchone()[0]
        if count == len(items):
            print("All items are already in the database.")
            items = False
        else:
            print(f"There are {len(items) - count} items to be uploaded.")
            query_del_heroes = """
                    TRUNCATE ITEMS
                    """
            cur_pg_hook.execute(query_del_heroes)
        con_pg_hook.close()
        return items

    @task(multiple_outputs=True)
    def upload_data_items(data_items: list | bool):
        if data_items is False:
            return print("ALL items are UPLOAD")

        for item in data_items:
            try:
                hook = PostgresHook(postgres_conn_id="postgres")
                hook.insert_rows(table="items",
                                 rows=[(item["id"], item["name"], item["cost"], item["secret_shop"],
                                        item["side_shop"], item["recipe"], item["localized_name"],)],
                                 target_fields=["id", "name", "cost", "secret_shop", "side_shop", "recipe",
                                                "localized_name"])
            except Exception as e:
                print(e)

    data_item = api_get_heroes()
    data_item = check_count_items(data_item)
    upload_data_items(data_item)


dag_download_upload_items()
