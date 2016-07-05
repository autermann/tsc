
DROP TABLE IF EXISTS sensors;
DROP TABLE IF EXISTS tracks;
DROP TABLE IF EXISTS trajectories;

CREATE TABLE sensors (
  id SERIAL PRIMARY KEY,
  objectid CHAR(24) UNIQUE NOT NULL
);

CREATE TABLE tracks (
  id SERIAL PRIMARY KEY,
  objectid CHAR(24) UNIQUE NOT NULL,
  start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  geom GEOMETRY(LineString, 4326) NOT NULL
);

CREATE TABLE trajectories (
  id SERIAL PRIMARY KEY,
  track INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
  start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  geom GEOMETRY(LineString, 4326) NOT NULL
);


CREATE INDEX ON measurements (track);
CREATE INDEX ON measurements (sensor);
CREATE INDEX ON measurements (time);
CREATE INDEX ON measurements USING GIST(geom) WITH (FILLFACTOR = 100);
CREATE INDEX ON tracks USING GIST(geom) WITH (FILLFACTOR = 100);
CREATE INDEX ON tracks (start_time);
CREATE INDEX ON tracks (end_time);
CREATE INDEX ON trajectories (track);
CREATE INDEX ON trajectories USING GIST (geom) WITH (FILLFACTOR = 100);
CREATE INDEX ON trajectories (start_time);
CREATE INDEX ON trajectories (end_time);

-- add an OID to be recognized by ArcGIS
ALTER TABLE measurements RENAME COLUMN id to objectid;
ALTER TABLE measurements DROP CONSTRAINT measurements_pkey;
ALTER TABLE measurements ADD COLUMN id SERIAL PRIMARY KEY;

-- fill the sensors table
INSERT INTO sensors (objectid)
  SELECT DISTINCT sensor FROM measurements ORDER BY sensor;

-- replace the sensor id with an reference
ALTER TABLE measurements ADD COLUMN sensor_id INTEGER;
UPDATE measurements AS m SET sensor_id = (SELECT s.id FROM sensors AS s WHERE s.objectid = m.sensor);
ALTER TABLE measurements DROP COLUMN sensor;
ALTER TABLE measurements RENAME COLUMN sensor_id TO sensor;
ALTER TABLE measurements ADD FOREIGN KEY (sensor) REFERENCES sensors (id) ON DELETE CASCADE;
ALTER TABLE measurements ALTER COLUMN sensor SET NOT NULL;

-- recreate the index
CREATE INDEX ON measurements (sensor);

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
UPDATE measurements AS m SET track_id = (SELECT t.id FROM tracks AS t WHERE t.objectid = m.track);
ALTER TABLE measurements DROP COLUMN track;
ALTER TABLE measurements RENAME COLUMN track_id TO track;
ALTER TABLE measurements ADD FOREIGN KEY (track) REFERENCES tracks (id) ON DELETE CASCADE;
ALTER TABLE measurements ALTER COLUMN track SET NOT NULL;

-- recreate the index
CREATE INDEX ON measurements (track);

-- remove tracks with less than 2 measurements
DELETE FROM tracks WHERE NOT ST_IsValid(geom);

WITH
  t1 AS (
    -- create the basic trajectories, this may contain
    -- lines with equal start and end point
    SELECT track,
      (ROW_NUMBER() OVER w)-1 AS rank,
      LAG(time) OVER w AS start_time,
      time AS end_time,
      ST_SetSRID(ST_MakeLine(LAG(geom) OVER w, geom), 4326) AS geom
    FROM measurements
    WINDOW w AS (
      PARTITION BY track
      ORDER BY time
    )
    ORDER BY track, time
    OFFSET 1
  ),
  t2 AS (
    -- transform lines with equal start and end
    -- point to single point geometries
    SELECT track, rank, start_time, end_time,
      (CASE
        WHEN ST_NumPoints(geom) = 2
          AND ST_StartPoint(geom) = ST_Endpoint(geom)
        THEN ST_StartPoint(geom)
        ELSE geom
      END) AS geom
    FROM t1 AS foo
  ),
  t3 AS (
    -- build up a grouping, first assign '1' to every geometry
    -- that is not equal to it's predecessor and then sum up
    -- the 1's to get a grouping key:
    -- 1|1; 1|2; 1|3; 0|3; 0|3; 1|4; 0|4; 0|4; 1|5; 1|6
    SELECT *, SUM(r) OVER (PARTITION BY track ORDER BY rank) AS grp
    FROM (
      SELECT *, (
        CASE WHEN ST_OrderingEquals(geom, LAG(geom) OVER (PARTITION BY track ORDER BY rank))
        THEN 0 ELSE 1 END) AS r
      FROM t2 AS foo
    ) AS foo
    ORDER BY track, rank
  ),
  t4 AS (
    -- merge features that have the same geometry
    SELECT
      track,
      ROW_NUMBER() OVER (PARTITION BY track ORDER BY MIN(rank)) AS rank,
      MIN(start_time) AS start_time,
      MAX(end_time) AS end_time,
      -- MIN is here just used to get any geometry (they're all the same)
      MIN(geom) AS geom,
      grp
    FROM t3 AS foo
    GROUP BY track, grp
    ORDER BY track, rank
  ),
  t5 AS (
    -- extend the time interval of lines before and after a point
    -- to include the time interval of the point. by this the
    -- trajectories will overlap, but this is intended
    SELECT track, rank,
      (
        CASE
          WHEN GeometryType(LAG(geom) OVER w) = 'POINT'
          THEN LAG(start_time) OVER w
          ELSE start_time
        END
      ) AS start_time,
      (
        CASE
          WHEN GeometryType(LEAD(geom) OVER w) = 'POINT'
          THEN LEAD(end_time) OVER w
          ELSE end_time
        END
      ) AS end_time,
      geom
    FROM t4 AS foo
    WINDOW w AS (
      PARTITION BY track
      ORDER BY rank
      ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    )
    ORDER BY track, rank
  ),
  t6 AS (
    -- as a last step throw out the points
    SELECT track, start_time, end_time, geom
    FROM t5 AS foo
    WHERE GeometryType(geom) = 'LINESTRING'
    ORDER BY track, rank
  )
INSERT INTO trajectories (track, start_time, end_time, geom)
  SELECT track, start_time, end_time, geom FROM t6;
