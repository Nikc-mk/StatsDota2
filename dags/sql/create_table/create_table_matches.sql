-- создать таблицу matches, если она не существует
CREATE TABLE IF NOT EXISTS matches (
    match_id BIGSERIAL PRIMARY KEY,
    radiant_win BOOLEAN,
    start_time INTEGER,
    duration INTEGER,
    first_blood_time INTEGER,
    lobby_type INTEGER,
    game_mode INTEGER,
    engine INTEGER,
    radiant_team_id INTEGER,
    dire_team_id INTEGER,
    radiant_name VARCHAR,
    dire_name VARCHAR,
    radiant_captain BIGSERIAL,
    dire_captain BIGSERIAL,
--    radiant_gold_adv INTEGER[],
--    radiant_xp_adv INTEGER[],
    patch INTEGER,
    radiant_score INTEGER,
    dire_score INTEGER
    )
    ;