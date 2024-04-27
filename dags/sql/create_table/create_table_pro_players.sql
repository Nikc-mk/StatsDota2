-- создать таблицу pro_players, если она не существует
CREATE TABLE IF NOT EXISTS pro_players (
    account_id BIGSERIAL PRIMARY KEY,
    persona_name VARCHAR DEFAULT NULL,
    loc_country_code VARCHAR DEFAULT NULL,
    name VARCHAR DEFAULT NULL,
    fantasy_role INTEGER DEFAULT NULL,
    team_id INTEGER,
    last_login TIMESTAMP DEFAULT NULL,
    CONSTRAINT fk_pro_players FOREIGN KEY (team_id) REFERENCES pro_teams(team_id));