-- LLD правила: сколько требуют обновления (nextcheck наступил), сколько ещё нет.
--
-- Как работает zabbix-server:
--   1. LLD-правило — это item с flags = 1 (ZBX_FLAG_DISCOVERY_RULE).
--   2. nextcheck хранится ТОЛЬКО в памяти сервера (структура ZBX_DC_ITEM),
--      в БД его нет. Он вычисляется как calculate_item_nextcheck(seed, delay, now)
--      — по сути это выравнивание lastclock + delay по сетке интервала.
--   3. Главная проверка в коде: dbconfig.c:9420 — if (dc_item->nextcheck > now) break;
--      Если nextcheck <= now, item извлекается из очереди и отправляется поллеру.
--   4. Последнее время обработки LLD-правила отражается в item_discovery.lastcheck
--      (MAX по дочерним itemid с данным parent_itemid = LLD rule itemid).
--   5. Аппроксимация nextcheck из БД:
--        nextcheck ≈ lastcheck + delay_seconds
--      где delay_seconds парсится из поля items.delay (форматы: "300", "5m", "1h", "1d").
--
-- Запрос работает для PostgreSQL.
-- Учитываются только активные LLD-правила (status = 0) на активных хостах (status = 0).
-- Правила на шаблонах (hosts.status = 3) исключены.

WITH lld_rules AS (
    SELECT
        i.itemid,
        i.name,
        i.delay,
        i.status,
        h.host,
        h.status AS host_status
    FROM items i
    JOIN hosts h ON h.hostid = i.hostid
    WHERE
        i.flags = 1          -- ZBX_FLAG_DISCOVERY_RULE
        AND i.status = 0     -- ITEM_STATUS_ACTIVE
        AND h.status = 0     -- HOST_STATUS_MONITORED
),

-- Последнее время когда LLD-правило отработало (MAX lastcheck среди дочерних элементов)
last_run AS (
    SELECT
        parent_itemid,
        MAX(lastcheck) AS lastcheck
    FROM item_discovery
    GROUP BY parent_itemid
),

-- Парсинг поля delay в секунды.
-- Поддерживаемые форматы: "300" (секунды), "5m", "1h", "1d", "1w".
-- Кастомные интервалы (";") и scheduling-интервалы ("md", "h", "m" со сложным форматом)
-- не поддерживаются в SQL — берётся только простой первый интервал до ";".
delay_parsed AS (
    SELECT
        r.itemid,
        r.name,
        r.host,
        r.delay,
        lr.lastcheck,
        -- Берём только первую часть до ";" (простой интервал)
        SPLIT_PART(r.delay, ';', 1) AS simple_delay_str,
        CASE
            WHEN SPLIT_PART(r.delay, ';', 1) ~ '^[0-9]+[wW]$' THEN
                (REGEXP_REPLACE(SPLIT_PART(r.delay, ';', 1), '[wW]', ''))::int * 604800
            WHEN SPLIT_PART(r.delay, ';', 1) ~ '^[0-9]+[dD]$' THEN
                (REGEXP_REPLACE(SPLIT_PART(r.delay, ';', 1), '[dD]', ''))::int * 86400
            WHEN SPLIT_PART(r.delay, ';', 1) ~ '^[0-9]+[hH]$' THEN
                (REGEXP_REPLACE(SPLIT_PART(r.delay, ';', 1), '[hH]', ''))::int * 3600
            WHEN SPLIT_PART(r.delay, ';', 1) ~ '^[0-9]+[mM]$' THEN
                (REGEXP_REPLACE(SPLIT_PART(r.delay, ';', 1), '[mM]', ''))::int * 60
            WHEN SPLIT_PART(r.delay, ';', 1) ~ '^[0-9]+[sS]?$' THEN
                (REGEXP_REPLACE(SPLIT_PART(r.delay, ';', 1), '[sS]', ''))::int
            ELSE NULL  -- нераспознанный формат (макросы, scheduling)
        END AS delay_seconds
    FROM lld_rules r
    LEFT JOIN last_run lr ON lr.parent_itemid = r.itemid
),

categorized AS (
    SELECT
        itemid,
        name,
        host,
        delay,
        delay_seconds,
        lastcheck,
        CASE
            WHEN delay_seconds IS NULL THEN 'unknown'   -- макрос или scheduling-интервал
            WHEN lastcheck IS NULL OR lastcheck = 0 THEN 'needs_update'  -- ни разу не запускалось
            WHEN (lastcheck + delay_seconds) <= EXTRACT(EPOCH FROM NOW())::int THEN 'needs_update'
            ELSE 'up_to_date'
        END AS update_status
    FROM delay_parsed
)

-- Итоговая статистика
SELECT
    COUNT(*)                                          AS total_lld_rules,
    COUNT(*) FILTER (WHERE update_status != 'needs_update'
                       AND update_status != 'unknown') AS up_to_date,
    COUNT(*) FILTER (WHERE update_status = 'needs_update'
                       OR  update_status = 'unknown')  AS needs_update
FROM categorized;

-- Детальный список (раскомментируйте при необходимости):
-- SELECT
--     host,
--     name,
--     delay,
--     delay_seconds,
--     TO_TIMESTAMP(lastcheck)              AS last_run_at,
--     TO_TIMESTAMP(lastcheck + delay_seconds) AS next_run_at,
--     update_status
-- FROM categorized
-- ORDER BY update_status DESC, host, name;
