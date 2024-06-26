from datetime import timedelta

import pendulum
import tenacity
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LIMIT_MATCH_ID = 50  # Установить длину списка match_id  для загрузки


@dag(
    # schedule="@once",
    schedule_interval=timedelta(minutes=10),
    start_date=pendulum.datetime(2024, 5, 10, tz="UTC"),
    catchup=False,
    tags=["upload"],
)
def dag_download_upload_pro_matches():
    """
    Эта функция определяет DAG для загрузки и сохранения статистики профессиональных матчей Dota 2.
    DAG состоит из пяти основных задач: получение списка профессиональных матчей, загрузка полной статистики матчей,
    сохранение информации о командах, сохранение информации о матчах и сохранение информации об игроках.
    """
    list_last_50 = SQLExecuteQueryOperator(
        task_id="get_pro_matches_from_tepm_table",
        conn_id="postgres",
        autocommit=True,
        database="postgres",
        sql=f"""SELECT match_id from temp_promatches_download_queue
        order by match_id desc
        limit {LIMIT_MATCH_ID}
        """,
        show_return_value_in_logs=False,
        do_xcom_push=True,
    )

    @task
    def download_pro_matches_data(**kwargs):
        """
        Загружает полную статистику профессиональных матчей из API OpenDota.
        Проверяет, существует ли матч в базе данных, и если нет, запрашивает данные матча.
        Возвращает список полной статистики профессиональных матчей.
        """
        data_pro_matches = list()
        pro_matches = kwargs["ti"].xcom_pull(task_ids="get_pro_matches_from_tepm_table")
        for pro_match in pro_matches:
            pg_hook = PostgresHook(postgres_conn_id="postgres")
            con_pg_hook = pg_hook.get_conn()
            cur_pg_hook = con_pg_hook.cursor()
            query = f"""
        SELECT exists (SELECT 1 FROM player_matches WHERE match_id = {pro_match[0]} LIMIT 1)
        """
            cur_pg_hook.execute(query)
            check_pro_match = cur_pg_hook.fetchone()[0]
            con_pg_hook.close()
            print(f"Check id proMatches: {check_pro_match}")
            if check_pro_match:
                print(f"Match {pro_match[0]} is already in the database.")
                continue
                # запрашиваю у апи opendota информацию о матче

            hook = HttpHook(method="GET", http_conn_id="opendota")
            retry_args = dict(
                wait=tenacity.wait_fixed(
                    30
                ),  # timeout если получен ответ не 2хх или 3ххх
                stop=tenacity.stop_after_attempt(10),
                retry=tenacity.retry_if_exception_type(Exception),
            )
            response = hook.run_with_advanced_retry(
                endpoint=f"/api/matches/{pro_match[0]}",
                _retry_args=retry_args,
            )
            pro_match_full_stat = response.json()
            print(f"ЗАПИСАЛИ МАТЧ:{pro_match_full_stat['match_id']}")
            data_pro_matches.append(pro_match_full_stat)

        return data_pro_matches

    @task()
    def upload_pro_teams(**kwargs):
        data_pro_matches = kwargs["ti"].xcom_pull(task_ids="download_pro_matches_data")
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        # Записываем информацию о командах
        for pro_match in data_pro_matches:
            pg_hook.insert_rows(
                table="pro_teams",
                replace=True,
                replace_index="team_id",
                rows=[
                    (
                        pro_match["radiant_team_id"],
                        pro_match["radiant_name"],
                    ),
                    (
                        pro_match["dire_team_id"],
                        pro_match["dire_name"],
                    ),
                ],
                target_fields=["team_id", "name"],
            )

    @task()
    def upload_pro_matches(**kwargs):
        """
        Загружает информацию о профессиональных матчах в базу данных PostgreSQL.

        Параметры:
        data_pro_matches (list): Список словарей, каждый из которых содержит полную статистику
        одного профессионального матча.

        Использует:
        PostgresHook: для подключения к базе данных PostgreSQL.

        Возвращает:
        None
        """
        data_pro_matches = kwargs["ti"].xcom_pull(task_ids="download_pro_matches_data")
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        # Записываем информацию о матчах
        for pro_match in data_pro_matches:
            try:
                pg_hook.insert_rows(
                    table="matches",
                    replace=True,
                    replace_index="match_id",
                    rows=[
                        (
                            pro_match["match_id"],
                            pro_match["radiant_win"],
                            pro_match["start_time"],
                            pro_match["duration"],
                            pro_match["first_blood_time"],
                            pro_match["lobby_type"],
                            pro_match["game_mode"],
                            pro_match["engine"],
                            pro_match["radiant_team_id"],
                            pro_match["dire_team_id"],
                            pro_match["radiant_name"],
                            pro_match["dire_name"],
                            pro_match["radiant_captain"],
                            pro_match["dire_captain"],
                            # pro_match["radiant_gold_adv"],
                            # pro_match["radiant_xp_adv"],
                            pro_match["patch"],
                            pro_match["radiant_score"],
                            pro_match["dire_score"],
                        )
                    ],
                    target_fields=[
                        "match_id",
                        "radiant_win",
                        "start_time",
                        "duration",
                        "first_blood_time",
                        "lobby_type",
                        "game_mode",
                        "engine",
                        "radiant_team_id",
                        "dire_team_id",
                        "radiant_name",
                        "dire_name",
                        "radiant_captain",
                        "dire_captain",
                        # "radiant_gold_adv", "radiant_xp_adv",
                        "patch",
                        "radiant_score",
                        "dire_score",
                    ],
                )
            except Exception as e:
                print(e)
            print(f"ИНФОРМАЦИЯ О МАТЧЕ {pro_match['match_id']} ЗАПИСАНА!")

    @task()
    def upload_player_matches(**kwargs):
        """
        Загружает информацию об игроках в профессиональных матчах в базу данных PostgreSQL.

        Параметры:
        data_pro_matches (list): Список словарей, каждый из которых содержит полную статистику
        одного профессионального матча.

        Использует:
        PostgresHook: для подключения к базе данных PostgreSQL.

        Возвращает:
        None
        """
        data_pro_matches = kwargs["ti"].xcom_pull(task_ids="download_pro_matches_data")
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        # записываем информацию о матчах
        for pro_match in data_pro_matches:
            for i in range(10):
                try:
                    pg_hook.insert_rows(
                        table="player_matches",
                        rows=[
                            (
                                pro_match["match_id"],
                                pro_match["players"][i]["account_id"],
                                i,
                                pro_match["players"][i]["hero_id"],
                                pro_match["players"][i]["item_0"],
                                pro_match["players"][i]["item_1"],
                                pro_match["players"][i]["item_2"],
                                pro_match["players"][i]["item_3"],
                                pro_match["players"][i]["item_4"],
                                pro_match["players"][i]["item_5"],
                                pro_match["players"][i]["kills"],
                                pro_match["players"][i]["deaths"],
                                pro_match["players"][i]["assists"],
                                pro_match["players"][i]["leaver_status"],
                                pro_match["players"][i]["gold"],
                                pro_match["players"][i]["last_hits"],
                                pro_match["players"][i]["denies"],
                                pro_match["players"][i]["gold_per_min"],
                                pro_match["players"][i]["xp_per_min"],
                                pro_match["players"][i]["gold_spent"],
                                pro_match["players"][i]["hero_damage"],
                                pro_match["players"][i]["tower_damage"],
                                pro_match["players"][i]["hero_healing"],
                                pro_match["players"][i]["level"],
                                pro_match["players"][i]["stuns"],
                                pro_match["players"][i]["gold_t"],
                                pro_match["players"][i]["lh_t"],
                                pro_match["players"][i]["xp_t"],
                                pro_match["players"][i]["creeps_stacked"],
                                pro_match["players"][i]["camps_stacked"],
                                pro_match["players"][i]["lane"],
                                pro_match["players"][i]["is_roaming"],
                                pro_match["players"][i]["roshans_killed"],
                                pro_match["players"][i]["observers_placed"],
                                pro_match["players"][i]["dn_t"],
                                pro_match["players"][i]["item_neutral"],
                                pro_match["players"][i]["net_worth"],
                            )
                        ],
                        target_fields=[
                            "match_id",
                            "account_id",
                            "player_slot",
                            "hero_id",
                            "item_0",
                            "item_1",
                            "item_2",
                            "item_3",
                            "item_4",
                            "item_5",
                            "kills",
                            "deaths",
                            "assists",
                            "leaver_status",
                            "gold",
                            "last_hits",
                            "denies",
                            "gold_per_min",
                            "xp_per_min",
                            "gold_spent",
                            "hero_damage",
                            "tower_damage",
                            "hero_healing",
                            "level",
                            "stuns",
                            "gold_t",
                            "lh_t",
                            "xp_t",
                            "creeps_stacked",
                            "camps_stacked",
                            "lane",
                            "is_roaming",
                            "roshans_killed",
                            "observers_placed",
                            "dn_t",
                            "item_neutral",
                            "net_worth",
                        ],
                    )
                except Exception as e:
                    print(e)
                print(
                    f"ИНФОРМАЦИЯ О ИГРОКЕ СЛОТ №{i} В МАТЧЕ {pro_match["match_id"]} ЗАПИСАНА!"
                )

    del_upload_match_id = SQLExecuteQueryOperator(
        task_id="del_upload_match_id",
        conn_id="postgres",
        autocommit=True,
        database="postgres",
        # решил, что зачем получать список на удаление через xcom, если вложенный запрос получит тот же список
        sql=f"""DELETE FROM temp_promatches_download_queue
        WHERE match_id IN (SELECT match_id from temp_promatches_download_queue
        order by match_id desc
        limit {LIMIT_MATCH_ID});
        """,
        show_return_value_in_logs=True,
    )
    download_pro_matches_data = download_pro_matches_data()
    u_pro_teams = upload_pro_teams()
    u_pro_matches = upload_pro_matches()
    u_player_matches = upload_player_matches()

    list_last_50 >> download_pro_matches_data
    download_pro_matches_data >> [u_pro_teams, u_pro_matches]
    u_pro_matches >> u_player_matches >> del_upload_match_id


dag_download_upload_pro_matches()
