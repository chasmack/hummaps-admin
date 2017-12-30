WITH q1 AS (
  SELECT m.id,
    array_agg(concat_ws(' ',
      'S' || trs.sec::text,
      hummaps_staging.township_str(trs.tshp),
      hummaps_staging.range_str(trs.rng)
    )) trs
  FROM hummaps_staging.map m
  JOIN hummaps_staging.trs trs ON trs.map_id = m.id
  GROUP BY m.id
  HAVING bit_or(subsec) IS NULL
)
SELECT m.id,
  unnest(regexp_matches(description, '(?:(?:[NS][EW]/4|[NSEW]/2)\s*)?(?:[NS][EW]/4|[NSEW]/2)\s*S\d{1,2}|\d+[NS]\s*,\s*\d+[EW]', 'ig')) part,
   m.description, q1.trs
FROM q1
JOIN hummaps_staging.map m USING (id)
WHERE m.description ~* '(?:(?:[NS][EW]/4|[NSEW]/2)\s*)?(?:[NS][EW]/4|[NSEW]/2)\s*S\d{1,2}'
;