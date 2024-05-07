-- создать таблицу pro_matches, если она не существует
CREATE TABLE IF NOT EXISTS matches (
    match_id BIGSERIAL PRIMARY KEY,
    radiant_win BOOLEAN,
    start_time INTEGER,
    duration INTEGER DEFAULT 0,
    first_blood_time INTEGER,
    lobby_type INTEGER,
    game_mode INTEGER,
    engine INTEGER,
    radiant_team_id INTEGER,
    dire_team_id INTEGER,
    radiant_team_name VARCHAR SMALLINT,
    dire_team_name VARCHAR SMALLINT,
    radiant_captain BIGSERIAL,
    dire_captain BIGSERIAL,
    radiant_gold_adv INTEGER[],
    radiant_xp_adv INTEGER[],
    version INTEGER,
    radiant_score INTEGER,
    dire_score INTEGER
    )
--    , CONSTRAINT fk_radiant_ FOREIGN KEY (radiant_team_id) REFERENCES pro_teams(team_id),
--    CONSTRAINT fk_dire_ FOREIGN KEY (dire_team_id) REFERENCES pro_teams(team_id))
    ;