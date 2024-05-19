-- создать таблицу временную таблицу очередь на загрузку проф матчей, если она не существует
CREATE TABLE IF NOT EXISTS temp_promatches_download_queue (
    match_id INTEGER PRIMARY KEY
    );
