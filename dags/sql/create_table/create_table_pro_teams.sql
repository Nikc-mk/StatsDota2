-- создать таблицу pro_teams, если она не существует
CREATE TABLE IF NOT EXISTS pro_teams (
    team_id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL);