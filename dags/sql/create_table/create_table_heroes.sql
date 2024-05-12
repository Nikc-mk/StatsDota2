-- создать таблицу heroes, если она не существует
CREATE TABLE IF NOT EXISTS heroes (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    localized_name VARCHAR NOT NULL,
    primary_attr VARCHAR,
    attack_type VARCHAR NOT NULL,
    roles text[]);