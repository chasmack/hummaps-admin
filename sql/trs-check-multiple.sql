-- Count redundant trs records.
-- Each map should have at most one trs record per section.
SELECT nrecs, count(nrecs) "sec refs",
    nrecs * count(nrecs) "trs recs", sum(nrecs - 1) redundant
FROM (
    SELECT count(*) nrecs
    FROM hummaps.trs
    GROUP BY map_id, tshp, rng, sec
) q1
GROUP BY nrecs
ORDER BY nrecs
;

--  nrecs | sec refs | trs recs | redundant
-- -------+----------+----------+-----------
--      1 |    60795 |    60795 |         0
--      2 |    18178 |    36356 |     18178
--      3 |     1547 |     4641 |      3094
--      4 |        8 |       32 |        24
-- (4 rows)