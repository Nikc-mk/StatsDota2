-- создать таблицу временную таблицу очередь на загрузку проф матчей, если она не существует BIGSERIAL
CREATE TABLE IF NOT EXISTS temp_promatches_download_queue (
    match_id BIGSERIAL PRIMARY KEY
    );
