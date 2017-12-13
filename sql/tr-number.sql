\pset format unaligned
\pset fieldsep '\t'
-- \pset tuples_only on
SELECT 
  t.maptype MAPTYPE, m.book BOOK, m.page PAGE,
  regexp_replace(m.client, '.*\(TR(\d+)\)(.*)', '\1\2') TRACT
FROM hummaps.map m
JOIN hummaps.maptype t ON m.maptype_id = t.id
WHERE t.abbrev = 'RM' AND m.client ~* '(TR\d+)'
ORDER BY m.book, m.page
;