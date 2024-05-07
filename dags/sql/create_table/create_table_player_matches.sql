-- создать таблицу pro_players, если она не существует
CREATE TABLE IF NOT EXISTS player_matches (
    account_id BIGSERIAL,
    player_slot INTEGER,
    hero_id INTEGER,
    item_0 INTEGER,
    item_1 INTEGER,
    item_2 INTEGER,
    item_3 INTEGER,
    item_4 INTEGER,
    item_5 INTEGER,
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    leaver_status INTEGER,
    gold INTEGER,
    last_hits INTEGER,
    denies INTEGER,
    gold_per_min INTEGER,
    xp_per_min INTEGER,
    gold_spent INTEGER,
    hero_damage INTEGER,
    tower_damage INTEGER,
    hero_healing INTEGER,
    'level' INTEGER,
    stuns REAL,
    gold_t INTEGER[],
    lh_t INTEGER[],
    xp_t INTEGER[],
    creeps_stacked INTEGER,
    camps_stacked INTEGER,
    lane INTEGER,
    is_roaming BOOLEAN,
    roshans_killed INTEGER,
    observers_placed INTEGER,
    dn_t INTEGER[],
    item_neutral INTEGER,
    net_worth INTEGER)










    )
--    CONSTRAINT fk_pro_players FOREIGN KEY (team_id) REFERENCES pro_teams(team_id))
    ;