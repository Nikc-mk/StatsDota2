-- создать таблицу heroes, если она не существует
CREATE TABLE IF NOT EXISTS pro_teams (
    team_id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    tag VARCHAR DEFAULT NULL,
    wins INTEGER,
    losses INTEGER);