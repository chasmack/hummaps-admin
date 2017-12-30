WITH q1 AS (
SELECT t.maptype "Maptype", count(m.id) "Maps"
FROM hummaps.maptype t
LEFT JOIN hummaps.map m ON m.maptype_id = t.id
GROUP BY t.id
ORDER BY count(*) DESC
)
SELECT * FROM q1
UNION ALL
SELECT 'Total Maps'::text, count(*)
FROM hummaps.map
;

WITH q1 AS (
SELECT s.description "TRS Source", count(trs.id) "Records"
FROM hummaps.source s
LEFT JOIN hummaps.trs trs ON trs.source_id = s.id
GROUP BY s.id
ORDER BY s.id
)
SELECT * FROM q1
UNION ALL
SELECT 'Total TRS Records'::text, count(*)
FROM hummaps.trs
;

SELECT 'Surveyors'::text "Table", count(*) "Records"
FROM hummaps.surveyor
UNION ALL
SELECT 'Signed By'::text, count(*)
FROM hummaps.signed_by
;

WITH q1 AS (
SELECT 'Without Image Files'::text "CCs", count(DISTINCT cc.id) "Records"
FROM hummaps.cc cc
LEFT JOIN hummaps.cc_image i ON i.cc_id = cc.id
WHERE i.id IS NULL
), q2 AS (
SELECT 'With Image Files'::text, count(DISTINCT cc.id)
FROM hummaps.cc cc
LEFT JOIN hummaps.cc_image i ON i.cc_id = cc.id
WHERE i.id IS NOT NULL
)
SELECT * FROM q1
UNION ALL
SELECT * FROM q2
UNION ALL
SELECT 'Total'::text, count(*)
FROM hummaps.cc
;

SELECT 'Map Images'::text "Image Files", count(*) "Count"
FROM hummaps.map_image
UNION ALL
SELECT 'Scan Images'::text, count(*)
FROM hummaps.scan
UNION ALL
SELECT 'PDF Files'::text, count(*)
FROM hummaps.pdf
;

