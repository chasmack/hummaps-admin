
SELECT map_id,
  lpad(hummaps_staging.township_str(tshp), 3, '0') tshp,
  hummaps_staging.range_str(rng) rng,
  ',' || string_agg(sec::text, ',') || ',' sec
FROM (
    SELECT DISTINCT map_id, tshp, rng, sec
    FROM hummaps.trs
    WHERE source_id IN (0, 1)
    ORDER BY map_id, tshp, rng, sec
) q1
GROUP BY map_id, tshp, rng
ORDER BY map_id
LIMIT 100;
