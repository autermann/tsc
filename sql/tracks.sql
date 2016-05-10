DROP TABLE IF EXISTS tracks;

CREATE TABLE tracks (
  id SERIAL PRIMARY KEY,
  objectid CHAR(24) UNIQUE NOT NULL,
  start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  geom GEOMETRY(LineString, 4326) NOT NULL
);

CREATE INDEX ON tracks USING GIST(geom) WITH (FILLFACTOR = 100);
CREATE INDEX ON tracks (start_time);
CREATE INDEX ON tracks (end_time);

-- fill the tracks table
INSERT INTO tracks(objectid, start_time, end_time, geom)
  SELECT
    m.track AS objectid,
    MIN(m.time) AS start_time,
    MAX(m.time) AS end_time,
    ST_RemoveRepeatedPoints(
      ST_SetSRID(ST_MakeLine(m.geom ORDER BY time), 4326)
    ) AS geom
  FROM measurements AS m
  GROUP BY m.track
  ORDER BY m.track;

-- replace the track id with an reference
ALTER TABLE measurements ADD COLUMN track_id INTEGER;
UPDATE measurements AS m
	SET track_id = (SELECT t.id FROM tracks AS t WHERE t.objectid = m.track);
ALTER TABLE measurements DROP COLUMN track;
ALTER TABLE measurements RENAME COLUMN track_id TO track;
ALTER TABLE measurements ADD FOREIGN KEY (track) REFERENCES tracks (id) ON DELETE CASCADE;
ALTER TABLE measurements ALTER COLUMN track SET NOT NULL;

-- recreate the index
CREATE INDEX ON measurements (track);

-- remove tracks with less than 2 measurements
DELETE FROM tracks WHERE NOT ST_IsValid(geom);