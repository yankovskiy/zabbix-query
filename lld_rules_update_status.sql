-- LLD правила: сколько требуют обновления (nextcheck наступил), сколько ещё нет.
--
-- Как работает zabbix-server (dbconfig.c):
--   1. LLD-правило — это item с flags = 1 (ZBX_FLAG_DISCOVERY_RULE).
--   2. nextcheck хранится ТОЛЬКО в памяти сервера (ZBX_DC_ITEM.nextcheck), в БД его нет.
--   3. Главная проверка: if (dc_item->nextcheck > now) break;  [dbconfig.c:9420]
--      Если nextcheck <= now, item извлекается из очереди и отправляется поллеру.
--   4. nextcheck вычисляется как calculate_item_nextcheck(seed, delay, lastclock).
--      Аппроксимация из БД: nextcheck ≈ last_run + delay_seconds
--
-- Откуда берётся last_run из БД:
--   item_discovery.lastcheck обновляется при каждом запуске LLD для каждого
--   обнаруженного элемента. Цепочка:
--     LLD-правило (items.flags=1)
--       -> прототип (items.flags=2) через item_discovery (itemid=прототип, parent_itemid=LLD rule)
--         -> созданный item (items.flags=4) через item_discovery (itemid=созданный, parent_itemid=прототип)
--   MAX(lastcheck) по созданным items даёт время последнего запуска LLD-правила.
--
-- Запрос работает для PostgreSQL.
-- Учитываются только активные LLD-правила (status=0) на мониторируемых хостах (status=0).

WITH

-- Время последнего запуска каждого LLD-правила:
-- LLD rule -> прototипы (item_discovery: parent=lld, child=прototип)
--          -> созданные items (item_discovery: parent=прototип, child=созданный)
-- MAX(lastcheck) созданных items = время последнего запуска правила
lld_last_run AS (
    SELECT
        id_proto.parent_itemid     AS lld_rule_id,
        MAX(id_created.lastcheck)  AS last_run
    FROM item_discovery id_created
    JOIN items i_created ON i_created.itemid = id_created.itemid
                        AND i_created.flags  = 4   -- ZBX_FLAG_DISCOVERY_CREATED
    JOIN items i_proto   ON i_proto.itemid   = id_created.parent_itemid
                        AND i_proto.flags    = 2   -- ZBX_FLAG_DISCOVERY_PROTOTYPE
    JOIN item_discovery id_proto ON id_proto.itemid = i_proto.itemid
    WHERE id_created.lastcheck > 0
    GROUP BY id_proto.parent_itemid
),

-- LLD-правила с delay_seconds и last_run
lld_rules AS (
    SELECT
        i.itemid,
        i.name,
        h.host,
        i.delay,
        llr.last_run,
        -- Конвертация delay-строки в секунды (только простой интервал, до первого ";")
        -- Форматы: "300", "5m", "1h", "1d", "1w", "300s"
        CASE
            WHEN SPLIT_PART(i.delay, ';', 1) ~ '^[0-9]+[wW]$'  THEN
                (REGEXP_REPLACE(SPLIT_PART(i.delay, ';', 1), '[wWdDhHmMsS]', '', 'g'))::int * 604800
            WHEN SPLIT_PART(i.delay, ';', 1) ~ '^[0-9]+[dD]$'  THEN
                (REGEXP_REPLACE(SPLIT_PART(i.delay, ';', 1), '[wWdDhHmMsS]', '', 'g'))::int * 86400
            WHEN SPLIT_PART(i.delay, ';', 1) ~ '^[0-9]+[hH]$'  THEN
                (REGEXP_REPLACE(SPLIT_PART(i.delay, ';', 1), '[wWdDhHmMsS]', '', 'g'))::int * 3600
            WHEN SPLIT_PART(i.delay, ';', 1) ~ '^[0-9]+[mM]$'  THEN
                (REGEXP_REPLACE(SPLIT_PART(i.delay, ';', 1), '[wWdDhHmMsS]', '', 'g'))::int * 60
            WHEN SPLIT_PART(i.delay, ';', 1) ~ '^[0-9]+[sS]?$' THEN
                (REGEXP_REPLACE(SPLIT_PART(i.delay, ';', 1), '[wWdDhHmMsS]', '', 'g'))::int
            ELSE NULL  -- макрос ({$VAR}) или scheduling-интервал — нельзя распарсить статически
        END AS delay_seconds
    FROM items i
    JOIN hosts h ON h.hostid = i.hostid
    LEFT JOIN lld_last_run llr ON llr.lld_rule_id = i.itemid
    WHERE
        i.flags  = 1  -- ZBX_FLAG_DISCOVERY_RULE
        AND i.status = 0  -- ITEM_STATUS_ACTIVE
        AND h.status = 0  -- HOST_STATUS_MONITORED
),

categorized AS (
    SELECT
        itemid,
        name,
        host,
        delay,
        delay_seconds,
        last_run,
        CASE
            -- delay=0: сбор отключён, правило не опрашивается планировщиком
            WHEN delay_seconds = 0                        THEN 'disabled_delay'
            -- delay содержит макрос или scheduling-интервал: не можем определить
            WHEN delay_seconds IS NULL                    THEN 'unknown_interval'
            -- ни разу не запускалось (нет обнаруженных элементов)
            WHEN last_run IS NULL OR last_run = 0         THEN 'needs_update'
            -- nextcheck ещё не наступил
            WHEN (last_run + delay_seconds) > EXTRACT(EPOCH FROM NOW())::int
                                                          THEN 'up_to_date'
            -- nextcheck уже прошёл
            ELSE                                               'needs_update'
        END AS update_status
    FROM lld_rules
)

-- Итоговая статистика
SELECT
    COUNT(*)                                                       AS total_lld_rules,
    COUNT(*) FILTER (WHERE update_status = 'up_to_date')           AS up_to_date,
    COUNT(*) FILTER (WHERE update_status = 'needs_update'
                        OR update_status = 'unknown_interval')     AS needs_update
FROM categorized;

-- Детальный список по каждому правилу (раскомментируйте при необходимости):
-- SELECT
--     host,
--     name,
--     delay,
--     delay_seconds,
--     TO_TIMESTAMP(last_run)                           AS last_run_at,
--     TO_TIMESTAMP(last_run + delay_seconds)           AS next_run_at,
--     EXTRACT(EPOCH FROM NOW())::int - last_run        AS seconds_since_last_run,
--     update_status
-- FROM categorized
-- ORDER BY update_status, host, name;
