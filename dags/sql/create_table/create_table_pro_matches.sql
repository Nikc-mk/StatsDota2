-- создать таблицу pro_matches, если она не существует
CREATE TABLE IF NOT EXISTS pro_matches (
    match_id BIGSERIAL PRIMARY KEY,
    duration INTEGER DEFAULT 0,
    start_time TIMESTAMP DEFAULT NULL,
    radiant_team_id INTEGER NOT NULL,
    radiant_name VARCHAR NOT NULL,
    dire_team_id INTEGER NOT NULL,
    dire_name VARCHAR NOT NULL,
    league_id INTEGER NOT NULL,
    league_name VARCHAR NOT NULL,
    radiant_score INTEGER DEFAULT 0,
    dire_score INTEGER DEFAULT 0,
    radiant_win BOOLEAN NOT NULL)
--    , CONSTRAINT fk_radiant_ FOREIGN KEY (radiant_team_id) REFERENCES pro_teams(team_id),
--    CONSTRAINT fk_dire_ FOREIGN KEY (dire_team_id) REFERENCES pro_teams(team_id))
    ;