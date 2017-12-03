WITH q1 AS (
    SELECT
        substring(imagefile from '.*/(.*)-') map,
        substring(imagefile from '/map/(..)') maptype
    FROM hummaps_staging.map_image
    WHERE map_id IS NULL
)
SELECT map "Not Indexed"
FROM (
SELECT distinct maptype, map
FROM q1
-- WHERE maptype NOT IN ('rm', 'rs')
ORDER BY maptype, map
) AS s1
;