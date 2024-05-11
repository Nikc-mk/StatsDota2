# import pendulum
# from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.http.hooks.http import HttpHook
#
#
# @dag(
#     schedule="@once",
#     start_date=pendulum.datetime(2024, 4, 26, tz="UTC"),
#     catchup=False,
#     tags=["upload"],
# )
# def dag_download_upload_pro_teams():
#     @task()
#     def api_get_pro_teams():
#         hook = HttpHook(method='GET', http_conn_id='opendota')
#         response = hook.run(endpoint="/api/teams")
#         pro_teams = response.json()
#         print(f"Fetched {len(pro_teams)} pro_teams.")
#
#         return pro_teams
#
#     @task
#     def check_count_pro_teams(pro_teams: list):
#         pg_hook = PostgresHook(postgres_conn_id="postgres")
#         con_pg_hook = pg_hook.get_conn()
#         cur_pg_hook = con_pg_hook.cursor()
#         query = """
#         SELECT COUNT(*) FROM pro_teams
#         """
#         cur_pg_hook.execute(query)
#         count = cur_pg_hook.fetchone()[0]
#         if count == len(pro_teams):
#             print("All pro_teams are already in the database.")
#             pro_teams = False
#         else:
#             print(f"There are {len(pro_teams) - count} pro_teams to be uploaded.")
#         con_pg_hook.close()
#         return pro_teams
#
#     @task(multiple_outputs=True)
#     def upload_data_pro_players(data_pro_teams: list | bool):
#         if data_pro_teams is False:
#             return print("ALL pro_teams are UPLOAD")
#
#         for pro_team in data_pro_teams:
#             try:
#                 src = PostgresHook(postgres_conn_id="postgres")
#                 src.insert_rows(table="pro_teams",
#                                 rows=[(pro_team["team_id"], pro_team["name"],
#                                        pro_team["tag"],
#                                        pro_team["wins"],
#                                        pro_team["losses"],)],
#                                 target_fields=["team_id", "name", "tag", "wins", "losses"])
#             except Exception as e:
#                 print(e)
#             print("Pro_teams updated successfully")
#
#     data_check_count_pro_players = api_get_pro_teams()
#     data_check_count_pro_players = check_count_pro_teams(data_check_count_pro_players)
#     upload_data_pro_players(data_check_count_pro_players)
#
#
# dag_download_upload_pro_teams()
