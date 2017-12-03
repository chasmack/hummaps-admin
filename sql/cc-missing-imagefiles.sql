SELECT t.maptype, m.book, m.page, cc.doc_number, cc_image.*
FROM hummaps_staging.cc
LEFT JOIN hummaps_staging.map m ON m.id = cc.map_id
JOIN hummaps_staging.maptype t ON t.id = m.maptype_id
LEFT JOIN hummaps_staging.cc_image ON cc.id = cc_image.cc_id
WHERE cc_id IS NULL
ORDER BY t.maptype, m.book DESC, m.page DESC
;