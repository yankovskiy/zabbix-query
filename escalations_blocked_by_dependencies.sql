-- Эскалации со статусом SKIP из-за зависимых триггеров
  SELECT
      e.escalationid,
      e.triggerid as escalation_trigger,
      t.description as escalation_description,
      h.name as escalation_hostname,
      to_timestamp(e.nextcheck) as last_nextcheck,
      -- Найти блокирующий триггер
      pt.triggerid as blocking_trigger,
      pt.description as blocking_description,
      ph.name as blocking_hostname
  FROM escalations e
  JOIN triggers t ON e.triggerid = t.triggerid
  JOIN functions f ON t.triggerid = f.triggerid
  JOIN items i ON f.itemid = i.itemid
  JOIN hosts h ON i.hostid = h.hostid
  -- Найти зависимости
  JOIN trigger_depends td ON t.triggerid =
  td.triggerid_down
  JOIN triggers pt ON td.triggerid_up = pt.triggerid
  JOIN functions pf ON pt.triggerid = pf.triggerid
  JOIN items pi ON pf.itemid = pi.itemid
  JOIN hosts ph ON pi.hostid = ph.hostid
  -- Проверить, что блокирующий триггер в проблеме
  JOIN problem p ON pt.triggerid = p.objectid
  WHERE e.status = 2  -- SKIP
    AND p.source = 0 AND p.object = 0  -- триггерные события
    AND p.r_eventid IS NULL  -- проблема активна
    AND e.nextcheck < EXTRACT(EPOCH FROM NOW()) - 3600  -- старше часа
  ORDER BY e.nextcheck ASC;
