SELECT source_id, source.description, count(*) nrec,
sum(
    (subsec >> 0 & 1) +
    (subsec >> 1 & 1) +
    (subsec >> 2 & 1) +
    (subsec >> 3 & 1) +
    (subsec >> 4 & 1) +
    (subsec >> 5 & 1) +
    (subsec >> 6 & 1) +
    (subsec >> 7 & 1) +
    (subsec >> 8 & 1) +
    (subsec >> 9 & 1) +
    (subsec >> 10 & 1) +
    (subsec >> 11 & 1) +
    (subsec >> 12 & 1) +
    (subsec >> 13 & 1) +
    (subsec >> 14 & 1) +
    (subsec >> 15 & 1) 
) bits
FROM hummaps.trs
JOIN hummaps.source ON source.id = trs.source_id
WHERE subsec IS NOT NULL
GROUP BY source_id, source.description
ORDER BY source_id
;

-- 
--  source_id |        description         | nrec  | bits
-- -----------+----------------------------+-------+-------
--          1 | Hollins subsection records | 11817 | 34910
--          3 | Parsed subsection records  |  9480 | 24623
--          4 | Additional XLSX records    |     1 |     4
-- (3 rows)
-- 