SELECT t.maptype, m.book, m.page, cc.doc_number
FROM hummaps.cc
JOIN hummaps.map m ON m.id = cc.map_id
JOIN hummaps.maptype t ON t.id = m.maptype_id
LEFT JOIN hummaps.cc_image i ON cc.id = i.cc_id
WHERE i.cc_id IS NULL
ORDER BY cc.doc_number DESC
;