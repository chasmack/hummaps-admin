WITH q1 AS (
  SELECT m.id map_id,
    s.firstname ||
    coalesce(' ' || left(s.secondname, 1), '') ||
    coalesce(' ' || left(s.thirdname, 1), '') ||
    ' ' || s.lastname ||
    coalesce(' ' || s.suffix, '') AS surveyor
  FROM hummaps.map m
  LEFT JOIN hummaps.signed_by sb ON sb.map_id = m.id
  LEFT JOIN hummaps.surveyor s ON sb.surveyor_id = s.id
  ORDER BY m.id, s.lastname, s.firstname
)
SELECT m.id map_id,
  coalesce(string_agg(q1.surveyor, ' & '), 'UNKNOWN') surveyors
FROM hummaps.map m
LEFT JOIN q1 ON q1.map_id = m.id
-- WHERE q1.surveyor IS NULL
GROUP BY m.id
HAVING count(*) > 1 
;
