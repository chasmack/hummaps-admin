WITH q1 AS (
SELECT cc_id, max(page) npages
FROM hummaps_staging.cc_image
GROUP BY cc_id
)
SELECT cc.map_id, cc.doc_number, cc.npages cc_pages, q1.npages pages, cc_image.imagefile
FROM hummaps_staging.cc
LEFT JOIN hummaps_staging.cc_image ON cc.id = cc_image.cc_id
LEFT JOIN q1 ON q1.cc_id = cc.id
WHERE cc.npages != q1.npages
;