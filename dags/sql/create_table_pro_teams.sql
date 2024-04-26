-- создать таблицу heroes, если она не существует
CREATE TABLE IF NOT EXISTS pro_teams (
    team_id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    tag VARCHAR NOT NULL,
    wins INTEGER NOT NULL,
    losses INTEGER NOT NULL);