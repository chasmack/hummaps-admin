
CREATE OR REPLACE FUNCTION
    hummaps_staging.trs2path(tshp integer, rng integer, sec integer, subsec integer)
    RETURNS SETOF ltree AS $$
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec))
        WHERE subsec IS NULL
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'A'))
        WHERE subsec & x'0001'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'B'))
        WHERE subsec & x'0002'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'C'))
        WHERE subsec & x'0004'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'D'))
        WHERE subsec & x'0008'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'E'))
        WHERE subsec & x'0010'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'F'))
        WHERE subsec & x'0020'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'G'))
        WHERE subsec & x'0040'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'H'))
        WHERE subsec & x'0080'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'I'))
        WHERE subsec & x'0100'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'J'))
        WHERE subsec & x'0200'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'K'))
        WHERE subsec & x'0400'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'L'))
        WHERE subsec & x'0800'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'M'))
        WHERE subsec & x'1000'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'N'))
        WHERE subsec & x'2000'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'O'))
        WHERE subsec & x'4000'::int > 0
        UNION
        SELECT text2ltree(concat_ws('.',
            hummaps_staging.township_str(tshp), hummaps_staging.range_str(rng), sec, 'P'))
        WHERE subsec & x'8000'::int > 0
    $$ LANGUAGE SQL
    IMMUTABLE
    ;

DROP TABLE IF EXISTS hummaps.trs_path;

CREATE TABLE hummaps.trs_path  (
    id serial PRIMARY KEY,
    map_id integer NOT NULL REFERENCES hummaps.map,
    trs_path ltree NOT NULL,
    source_id integer NOT NULL REFERENCES hummaps.source
);
GRANT SELECT ON TABLE hummaps.trs_path TO hummaps;

INSERT INTO hummaps.trs_path (map_id, trs_path, source_id)
SELECT map_id, hummaps_staging.trs2path(tshp, rng, sec, subsec) trs_path, source_id
FROM hummaps.trs
;

-- Delete trs full section records having a
-- corresponding trs subsection record.

WITH t1 AS (
    SELECT DISTINCT t1.map_id, subpath(t1.trs_path, 0, 3) trs_path
    FROM hummaps.trs_path t1
    WHERE nlevel(t1.trs_path) = 4
    ORDER BY map_id
)
DELETE FROM hummaps.trs_path
WHERE id IN (
    SELECT t2.id
    FROM hummaps.trs_path t2
    JOIN t1 USING (map_id, trs_path)
    WHERE nlevel(t2.trs_path) = 3
)
;

VACUUM FREEZE hummaps.trs_path;
CREATE INDEX hummaps_path_gist_idx ON hummaps.trs_path USING GIST (trs_path);

-- SELECT map_id, hummaps_staging.trs2path(tshp, rng, sec, subsec) trs_path, source_id,
--     hummaps_staging.township_str(tshp) tshp, hummaps_staging.range_str(rng) rng, sec,
--     '0x' || lpad(to_hex(subsec), 4, '0') subsec
-- FROM hummaps_staging.trs
-- WHERE map_id = 32
-- ;

--  map_id |  trs_path  | source_id | tshp | rng | sec | subsec
-- --------+------------+-----------+------+-----+-----+--------
--      32 | 4N.1W.2    |         0 | 4N   | 1W  |   2 |
--      32 | 4N.1W.3    |         0 | 4N   | 1W  |   3 |
--      32 | 5N.1W.25   |         0 | 5N   | 1W  |  25 |
--      32 | 5N.1W.26   |         0 | 5N   | 1W  |  26 |
--      32 | 5N.1W.27   |         0 | 5N   | 1W  |  27 |
--      32 | 5N.1W.34   |         0 | 5N   | 1W  |  34 |
--      32 | 5N.1W.35   |         0 | 5N   | 1W  |  35 |
--      32 | 5N.1W.36   |         0 | 5N   | 1W  |  36 |
--      32 | 4N.1W.2.A  |         1 | 4N   | 1W  |   2 | 0x0001
--      32 | 4N.1W.3.D  |         1 | 4N   | 1W  |   3 | 0x0008
--      32 | 5N.1W.25.M |         1 | 5N   | 1W  |  25 | 0x1000
--      32 | 5N.1W.26.M |         1 | 5N   | 1W  |  26 | 0xf000
--      32 | 5N.1W.26.N |         1 | 5N   | 1W  |  26 | 0xf000
--      32 | 5N.1W.26.O |         1 | 5N   | 1W  |  26 | 0xf000
--      32 | 5N.1W.26.P |         1 | 5N   | 1W  |  26 | 0xf000
--      32 | 5N.1W.27.P |         1 | 5N   | 1W  |  27 | 0x8000
--      32 | 5N.1W.34.D |         1 | 5N   | 1W  |  34 | 0x8888
--      32 | 5N.1W.34.H |         1 | 5N   | 1W  |  34 | 0x8888
--      32 | 5N.1W.34.L |         1 | 5N   | 1W  |  34 | 0x8888
--      32 | 5N.1W.34.P |         1 | 5N   | 1W  |  34 | 0x8888
--      32 | 5N.1W.35.A |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.B |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.C |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.D |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.E |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.F |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.G |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.H |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.I |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.J |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.K |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.L |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.35.M |         1 | 5N   | 1W  |  35 | 0x1fff
--      32 | 5N.1W.36.A |         1 | 5N   | 1W  |  36 | 0x0111
--      32 | 5N.1W.36.E |         1 | 5N   | 1W  |  36 | 0x0111
--      32 | 5N.1W.36.I |         1 | 5N   | 1W  |  36 | 0x0111
-- (36 rows)