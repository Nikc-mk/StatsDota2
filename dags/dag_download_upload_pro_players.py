import pendulum
import tenacity
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
    def api_get_pro_matches():
        hook = HttpHook(method='GET', http_conn_id='opendota')
        response = hook.run(endpoint="/api/proMatches")
        pro_matches = response.json()
        print(f"Fetched {len(pro_matches)} pro_players.")

        return pro_matches

    @task
    def download_pro_matches_data(pro_matches: list):
        data_pro_matches = list()
        pg_hook = PostgresHook(postgres_conn_id="stat_dota2")

        for pro_match in pro_matches:
            con_pg_hook = pg_hook.get_conn()
            cur_pg_hook = con_pg_hook.cursor()
            # проверяю по match_id, наличие матча в базе данных
            query = f"""
        SELECT exists (SELECT 1 FROM pro_matches WHERE match_id = {pro_match["match_id"]} LIMIT 1)
        """
            cur_pg_hook.execute(query)
            check_pro_match = cur_pg_hook.fetchone()[0]
            con_pg_hook.close()
            print(f"Check id proMatches: {check_pro_match}")
            if check_pro_match:
                print(f"Match {pro_match['match_id']} is already in the database.")
                continue
            # запрашиваю у апи opendota информацию о матче
            try:
                hook = HttpHook(method='GET', http_conn_id='opendota')
                retry_args = dict(
                    wait=tenacity.wait_fixed(30),  # timeout если получен ответ не 2хх или 3ххх
                    stop=tenacity.stop_after_attempt(10),
                    retry=tenacity.retry_if_exception_type(Exception),
                )
                response = hook.run_with_advanced_retry(endpoint=f"/api/matches/{pro_match["match_id"]}",
                                                        _retry_args=retry_args)
                pro_match_full_stat = response.json()
            except Exception as ex:
                print(f"ОШИБКА HttpHook: {ex}")
            print(f"ЗАПИСАЛИ МАТЧ:{pro_match_full_stat["match_id"]}")
            data_pro_matches.append(pro_match_full_stat)
        return data_pro_matches

    @task()
    def upload_pro_teams(data_pro_matches: list):
        pg_hook = PostgresHook(postgres_conn_id="stat_dota2")
        # записываем информацию о командах
        for pro_match in data_pro_matches:
            try:
                pg_hook.insert_rows(table="pro_teams", replace=True, replace_index="team_id",
                                    rows=[
                                        (pro_match["radiant_team_id"], pro_match["radiant_name"],),
                                        (pro_match["dire_team_id"], pro_match["dire_name"],)
                                    ],
                                    target_fields=["team_id", "name"])
            except Exception as ex:
                print(ex)

    #     # записываем информацию о игроках
    #     try:
    #         pg_hook.insert_rows(table="pro_players", replace=True, replace_index="account_id",
    #                             rows=[
    #                                 (proMatch["players"][0]["account_id"], proMatch["players"][0]["personaname"],
    #                                  proMatch["players"][0]["name"], proMatch["players"][0]["lane_role"],
    #                                  proMatch["radiant_team_id"],),
    #
    #                                 (proMatch["players"][5]["account_id"], proMatch["players"][5]["personaname"],
    #                                  proMatch["players"][5]["name"], proMatch["players"][5]["lane_role"],
    #                                  proMatch["dire_team_id"],)
    #
    #                             ],
    #                             target_fields=["account_id", "personaname", "name", "lane_role", "team_id"])
    #     except Exception as ex:
    #         print(ex)
    #     con_pg_hook.close()
    #
    # return pro_Matches

    # @task(multiple_outputs=True)
    # def upload_data_pro_players(data_pro_players: list | bool):
    #     if data_pro_players is False:
    #         return print("ALL pro_players are UPLOAD")
    #
    #     for pro_player in data_pro_players:
    #         try:
    #             src = PostgresHook(postgres_conn_id="stat_dota2")
    #             src.insert_rows(table="pro_players",
    #                             rows=[(pro_player["account_id"], pro_player["personaname"],
    #                                    pro_player["loccountrycode"],
    #                                    pro_player["name"],
    #                                    pro_player["fantasy_role"], pro_player["team_id"], pro_player["last_login"],)],
    #                             target_fields=["account_id", "personaname", "loccountrycode", "name", "fantasy_role",
    #                                            "team_id", "last_login"])
    #         except Exception as e:
    #             print(e)
    #         print("Pro_players updated successfully")

    lst_new_pro_matches = api_get_pro_matches()
    full_data_pro_matches = download_pro_matches_data(lst_new_pro_matches)
    upload_pro_teams(full_data_pro_matches)
    # upload_data_pro_players(data_check_count_pro_players)


dag_download_upload_pro_players()
