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
def dag_download_upload_pro_players():
    @task()
    def api_get_pro_players():
        hook = HttpHook(method='GET', http_conn_id='opendota')
        response = hook.run(endpoint="/api/proPlayers")
        pro_players = response.json()
        print(f"Fetched {len(pro_players)} pro_players.")

        return pro_players

    @task
    def check_count_pro_players(pro_players: list):
        pg_hook = PostgresHook(postgres_conn_id="stat_dota2")
        con_pg_hook = pg_hook.get_conn()
        cur_pg_hook = con_pg_hook.cursor()
        query = """
        SELECT COUNT(*) FROM pro_players
        """
        cur_pg_hook.execute(query)
        count = cur_pg_hook.fetchone()[0]
        if count == len(pro_players):
            print("All pro_players are already in the database.")
            pro_players = False
        else:
            print(f"There are {len(pro_players) - count} pro_players to be uploaded.")
            query_del_pro_players = """
                    TRUNCATE pro_players
                    """
            cur_pg_hook.execute(query_del_pro_players)
        con_pg_hook.close()
        return pro_players

    @task(multiple_outputs=True)
    def upload_data_pro_players(data_pro_players: list | bool):
        if data_pro_players is False:
            return print("ALL pro_players are UPLOAD")

        for pro_player in data_pro_players:
            try:
                src = PostgresHook(postgres_conn_id="stat_dota2")
                src.insert_rows(table="pro_players",
                                rows=[(pro_player["account_id"], pro_player["personaname"],
                                       pro_player["loccountrycode"],
                                       pro_player["name"],
                                       pro_player["fantasy_role"], pro_player["team_id"], pro_player["last_login"],)],
                                target_fields=["account_id", "personaname", "loccountrycode", "name", "fantasy_role",
                                               "team_id", "last_login"])
            except Exception as e:
                print(e)
            print("Pro_players updated successfully")

    data_check_count_pro_players = api_get_pro_players()
    data_check_count_pro_players = check_count_pro_players(data_check_count_pro_players)
    upload_data_pro_players(data_check_count_pro_players)


dag_download_upload_pro_players()
