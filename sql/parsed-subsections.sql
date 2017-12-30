SELECT
  map_id,
  hummaps_staging.subsec_str(subsec) || ' S' || sec::text sec,
  hummaps_staging.township_str(tshp) tshp,
  hummaps_staging.range_str(rng) rng,
  s.description source
FROM hummaps_staging.trs trs
JOIN hummaps_staging.source s ON trs.source_id = s.id
WHERE s.id = 3
-- ORDER BY trs.tshp desc, trs.rng desc, trs.sec asc, trs.map_id asc
ORDER BY trs.map_id
;