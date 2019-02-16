
DROP INDEX IF EXISTS hummaps_trs_btree_idx;

VACUUM FREEZE hummaps.trs;
CREATE INDEX hummaps_trs_btree_idx ON hummaps.trs USING BTREE (tshp, rng, sec, map_id);

-- DROP INDEX IF EXISTS trs_btree_idx;

-- VACUUM FREEZE hummaps_staging.trs;
-- CREATE INDEX trs_btree_idx ON hummaps_staging.trs USING BTREE (tshp, rng, sec, map_id);

-- DROP INDEX IF EXISTS path_gist_idx;

-- VACUUM FREEZE hummaps_staging.trs_path;
-- CREATE INDEX path_gist_idx ON hummaps_staging.trs_path USING GIST (trs_path);
