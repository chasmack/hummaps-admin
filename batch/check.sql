
SET search_path TO hummaps, public;

SELECT
    township_str(max(tshp)) t_max,
    township_str(min(tshp)) t_min,
    range_str(max(rng)) r_max,
    range_str(min(rng)) r_min
FROM hummaps.trs
;