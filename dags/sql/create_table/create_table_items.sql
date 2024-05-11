-- создать таблицу items, если она не существует
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    cost INTEGER,
    secret_shop INTEGER,
    side_shop INTEGER,
    recipe INTEGER,
    localized_name VARCHAR);
