SET search_path TO hummaps, pubic;

CREATE OR REPLACE FUNCTION hummaps.map_id(
    IN maptype text,
    IN book integer,
    IN page integer,
    OUT map_id integer
) AS $$
    SELECT m.id map_id
    FROM hummaps.map m
    JOIN hummaps.maptype t ON t.id = m.maptype_id
    WHERE
        t.abbrev = upper(maptype) AND
        m.book = book AND
        m.page <= page AND (m.page + m.npages) > page
    ;
$$ LANGUAGE SQL
IMMUTABLE
;

CREATE OR REPLACE FUNCTION hummaps.map_name(
    IN map_id integer,
    OUT map_name text
) AS $$
    SELECT
        lpad(m.book::text, 3, '0') || lower(t.abbrev) || lpad(m.page::text, 3, '0') map_name
    FROM hummaps.map m
    JOIN hummaps.maptype t ON t.id = m.maptype_id
    WHERE m.id = map_id
    ;
$$
LANGUAGE sql IMMUTABLE
;

CREATE OR REPLACE FUNCTION hummaps.township_number(
    IN tshp_str text,
    OUT tshp integer
) AS $$
    SELECT CASE
        WHEN t[1]::int = 0 THEN NULL
        WHEN t[2] = 'N' THEN t[1]::int + -1
        WHEN t[2] = 'S' THEN t[1]::int * -1
        ELSE NULL
    END tshp
    FROM (
        SELECT regexp_matches(upper(tshp_str), '^T?(\d+)([NS])$') t
    ) q1;
$$ LANGUAGE SQL
IMMUTABLE
;

CREATE OR REPLACE FUNCTION hummaps.township_str(
    IN tshp integer,
    OUT tshp_str text
) AS $$
    SELECT CASE
        WHEN tshp between   0 and 98 THEN (tshp +  1)::text || 'N'
        WHEN tshp between -99 and -1 THEN (tshp * -1)::text || 'S'
        ELSE NULL
    END tshp_str;
$$ LANGUAGE SQL
IMMUTABLE
;

CREATE OR REPLACE FUNCTION hummaps.range_number(
    IN rng_str text,
    OUT rng integer
) AS $$
    SELECT CASE
        WHEN r[1]::int = 0 THEN NULL
        WHEN r[2] = 'E' THEN r[1]::int + -1
        WHEN r[2] = 'W' THEN r[1]::int * -1
        ELSE NULL
    END rng
    FROM (
        SELECT regexp_matches(upper(rng_str), '^R?(\d+)([EW])$') r
    ) q1;
$$ LANGUAGE SQL
IMMUTABLE
;

CREATE OR REPLACE FUNCTION hummaps.range_str(
    IN rng integer,
    OUT rng_str text
) AS $$
    SELECT CASE
        WHEN rng between   0 and 98 THEN (rng +  1)::text || 'E'
        WHEN rng between -99 and -1 THEN (rng * -1)::text || 'W'
        ELSE NULL
    END rng_str;
$$ LANGUAGE SQL
IMMUTABLE
;

