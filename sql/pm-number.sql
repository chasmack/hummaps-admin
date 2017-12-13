\pset format unaligned
\pset fieldsep '\t'
-- \pset tuples_only on
SELECT 
  t.maptype MAPTYPE, m.book BOOK, m.page PAGE,
  trim(regexp_replace(m.client, '.*\(PM(\d+)\)\s*(\w*)', '\1 \2')) NUMBER
FROM hummaps.map m
JOIN hummaps.maptype t ON m.maptype_id = t.id
WHERE t.abbrev = 'PM'
ORDER BY m.book, m.page
;