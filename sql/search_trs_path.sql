SET search_path TO hummaps, public;

-- EXPLAIN ANALYZE
SELECT count(distinct map_id)
FROM trs t
WHERE
    t.tshp = hummaps_staging.township_number('5N') AND
    t.rng = hummaps_staging.range_number('1W') AND
    t.sec = 25 AND
    t.subsec & x'FFFF'::int > 0
;

-- EXPLAIN ANALYZE
SELECT count(distinct map_id)
FROM trs_path t
WHERE t.trs_path IN (
    '5N.1W.25.A',
    '5N.1W.25.B',
    '5N.1W.25.C',
    '5N.1W.25.D',
    '5N.1W.25.E',
    '5N.1W.25.F',
    '5N.1W.25.G',
    '5N.1W.25.H',
    '5N.1W.25.I',
    '5N.1W.25.J',
    '5N.1W.25.K',
    '5N.1W.25.L',
    '5N.1W.25.M',
    '5N.1W.25.N',
    '5N.1W.25.O',
    '5N.1W.25.P'
    )
;

-- EXPLAIN ANALYZE
SELECT count(distinct map_id)
FROM trs_path t
WHERE t.trs_path ? ARRAY[
    '5N.1W.25.A',
    '5N.1W.25.B',
    '5N.1W.25.C',
    '5N.1W.25.D',
    '5N.1W.25.E',
    '5N.1W.25.F',
    '5N.1W.25.G',
    '5N.1W.25.H',
    '5N.1W.25.I',
    '5N.1W.25.J',
    '5N.1W.25.K',
    '5N.1W.25.L',
    '5N.1W.25.M',
    '5N.1W.25.N',
    '5N.1W.25.O',
    '5N.1W.25.P'
]::lquery[]
;

-- EXPLAIN ANALYZE
SELECT count(distinct map_id)
FROM trs t
WHERE
    t.tshp = hummaps_staging.township_number('5N') AND
    t.rng = hummaps_staging.range_number('1W') AND
    t.sec IN (23,24,25,26)
;

-- EXPLAIN ANALYZE
SELECT count(distinct map_id)
FROM trs_path t
WHERE t.trs_path IN (
    '5N.1W.23',
    '5N.1W.24',
    '5N.1W.25',
    '5N.1W.26'
    )
;

-- EXPLAIN ANALYZE
SELECT count(distinct map_id)
FROM trs t
WHERE
    t.tshp = hummaps_staging.township_number('5N') AND
    t.rng = hummaps_staging.range_number('1W')
;

-- EXPLAIN ANALYZE
SELECT count(distinct map_id)
FROM trs_path t
WHERE t.trs_path <@ '5N.1W'
;

-- Subsection search spanning two townships
-- E/2 S36 T5N R1W + W/2 S31 T5N R1E
WITH q1 AS (
    SELECT DISTINCT map_id
    FROM trs t
    WHERE
        t.tshp = hummaps_staging.township_number('5N') AND
        t.rng = hummaps_staging.range_number('1W') AND
        t.sec = 36 AND
        t.subsec & x'cccc'::integer > 0
    OR
        t.tshp = hummaps_staging.township_number('5N') AND
        t.rng = hummaps_staging.range_number('1E') AND
        t.sec = 31 AND
        t.subsec & x'3333'::integer > 0
), q2 AS (
    SELECT DISTINCT map_id
    FROM trs_path t
    WHERE t.trs_path IN (
        '5N.1W.36.C',
        '5N.1W.36.D',
        '5N.1W.36.G',
        '5N.1W.36.H',
        '5N.1W.36.K',
        '5N.1W.36.L',
        '5N.1W.36.O',
        '5N.1W.36.P',
        '5N.1E.31.A',
        '5N.1E.31.B',
        '5N.1E.31.E',
        '5N.1E.31.F',
        '5N.1E.31.I',
        '5N.1E.31.J',
        '5N.1E.31.M',
        '5N.1E.31.N'
        )
)
-- Compare results from the two CTEs
SELECT q1.map_id
FROM q1
LEFT JOIN q2 USING (map_id)
WHERE q2.map_id IS NULL
UNION ALL
SELECT q2.map_id
FROM q2
LEFT JOIN q1 USING (map_id)
WHERE q1.map_id IS NULL
;
