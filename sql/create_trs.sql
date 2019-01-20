
CREATE TABLE trs  (
    id serial PRIMARY KEY,
    map_id integer NOT NULL REFERENCES map,
    tshp integer NOT NULL,
    rng integer NOT NULL,
    sec integer NOT NULL,
    subsec integer,
    CONSTRAINT valid_tshp CHECK (tshp BETWEEN -5 AND 14),
    CONSTRAINT valid_rng CHECK (rng BETWEEN -3 AND 7),
    CONSTRAINT valid_sec CHECK (sec BETWEEN 1 AND 36),
    CONSTRAINT valid_subsec CHECK (subsec BETWEEN 1 AND 65535),
    CONSTRAINT single_rec_per_sec UNIQUE (map_id, tshp, rng, sec)
);