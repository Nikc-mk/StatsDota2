import requests
import json


def get_public_matches(less_than_match_id: int = 7570292809,
                       min_rank: int = 60,
                       max_rank: int = 70):
    """
    :param less_than_match_id: Получить сведения о матчах меньче с ID меньше чем указанный
    :param min_rank: Минимальный ранг для матчей. Ранги представлены целыми числами (10-15: Вестник, 20-25:
                     Страж, 30-35: Крестоносец, 40-45: Архонт, 50-55: Легенда, 60-65: Древний, 70-75:
                     Божественный, 80- 85: Бессмертный). Каждое приращение представляет собой дополнительную
                     звезду.
    :param max_rank: Максимальный ранг за матчи. Ранги представлены целыми числами (10-15: Вестник, 20-25:
                     Страж, 30-35: Крестоносец, 40-45: Архонт, 50-55: Легенда, 60-65: Древний, 70-75:
                     Божественный, 80- 85: Бессмертный). Каждое приращение представляет собой дополнительную звезду.
    :return: JSON со статистикой по матчам
                    [
                {
                "match_id": 3703866531,
                "match_seq_num": 0,
                "radiant_win": true,
                "start_time": 0,
                "duration": 0,
                "lobby_type": 0,
                "game_mode": 0,
                "avg_rank_tier": 0,
                "num_rank_tier": 0,
                "cluster": 0,
                "radiant_team": [
                0
                ],
                "dire_team": [
                0]}
                ]
    """
    matches = requests.get(
        f"https://api.opendota.com/api/publicMatches?less_than_match_id={less_than_match_id}&"
        f"min_rank={min_rank}&max_rank={max_rank}"
    )
    return matches.json()


def parse_json_match(json_data: json):
    for i in json_data:
        match_id = i["match_id"]
        start_time = i["start_time"]
        duration = i["duration"]
        avg_rank_tier = i["avg_rank_tier"]
        num_rank_tier = i["num_rank_tier"]
        radiant_team_1 = i["radiant_team"][0]
        radiant_team_2 = i["radiant_team"][1]
        radiant_team_3 = i["radiant_team"][2]
        radiant_team_4 = i["radiant_team"][3]
        radiant_team_5 = i["radiant_team"][4]

        dire_team_1 = i["dire_team"][0]
        dire_team_2 = i["dire_team"][1]
        dire_team_3 = i["dire_team"][2]
        dire_team_4 = i["dire_team"][3]
        dire_team_5 = i["dire_team"][4]


json_data = get_public_matches()
parse_json_match(json_data)
