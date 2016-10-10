CREATE OR REPLACE FUNCTION interpolate_linear(DOUBLE PRECISION, DOUBLE PRECISION, INTEGER, INTEGER)
RETURNS DOUBLE PRECISION AS
$$ SELECT $1 + ($3/($4::numeric + 1)) * ($2-$1) $$
LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION epoch_to_timestamp(DOUBLE PRECISION)
RETURNS TIMESTAMP WITHOUT TIME ZONE AS
$$ SELECT TIMESTAMP WITHOUT TIME ZONE 'epoch' + $1 * INTERVAL '1 second' $$
LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION angular_distance(p1 GEOMETRY(Point, 4326), p2 GEOMETRY(Point, 4326))
RETURNS DOUBLE PRECISION AS $BODY$
  DECLARE
    a DOUBLE PRECISION;
  BEGIN
    a := power(sin(radians(ST_Y(p2)-ST_Y(p1))/2), 2) +
        cos(radians(ST_Y(p1))) * cos(radians(ST_Y(p2))) *
        power(sin(radians(ST_X(p2)-ST_X(p1))/2), 2);
    RETURN 2 * atan2(sqrt(a), sqrt(1-a));
  END
$BODY$ LANGUAGE plpgsql IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION intermediate_point(p1 GEOMETRY(Point, 4326), p2 GEOMETRY(Point, 4326), distance DOUBLE PRECISION, fraction DOUBLE PRECISION)
RETURNS GEOMETRY(Point, 4326) AS $BODY$
  DECLARE
    lon_1 DOUBLE PRECISION;
    lat_1 DOUBLE PRECISION;
    lon_2 DOUBLE PRECISION;
    lat_2 DOUBLE PRECISION;
    a DOUBLE PRECISION;
    b DOUBLE PRECISION;
    x DOUBLE PRECISION;
    y DOUBLE PRECISION;
    z DOUBLE PRECISION;
    lat DOUBLE PRECISION;
    lon DOUBLE PRECISION;
  BEGIN
    IF distance = 0 THEN
      RETURN p1;
    ELSE
      lon_1 := radians(ST_X(p1));
      lat_1 := radians(ST_Y(p1));
      lon_2 := radians(ST_X(p2));
      lat_2 := radians(ST_Y(p2));
      a := sin((1-fraction)*distance) / sin(distance);
      b := sin(fraction*distance) / sin(distance);
      x := a * cos(lat_1) * cos(lon_1) + b * cos(lat_2) * cos(lon_2);
      y := a * cos(lat_1) * sin(lon_1) + b * cos(lat_2) * sin(lon_2);
      z := a * sin(lat_1) + b * sin(lat_2);
      lon := atan2(y, x);
      lat := atan2(z, sqrt(power(x, 2) + power(y, 2)));
      RETURN ST_SetSRID(ST_MakePoint(degrees(lon), degrees(lat)), 4326);
    END IF;
  END
$BODY$ LANGUAGE plpgsql IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION intermediate_point(p1 GEOMETRY(Point, 4326), p2 GEOMETRY(Point, 4326), fraction DOUBLE PRECISION)
RETURNS GEOMETRY(Point, 4326) AS
$$ SELECT intermediate_point($1, $2, angular_distance($1, $2), $3); $$
LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION create_trajectories(bigint, bigint)
RETURNS TABLE(id integer, start_time timestamp without time zone, end_time timestamp without time zone, geom Geometry(LineString, 4326)) AS $$
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
      WHERE track >= $1 AND track < $2
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
    )
    -- as a last step throw out the points
    SELECT track, start_time, end_time, geom::Geometry(LineString, 4326)
    FROM t5 AS foo
    WHERE GeometryType(geom) = 'LINESTRING'
    ORDER BY track, rank
$$ LANGUAGE SQL IMMUTABLE;



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

-- densify...
ALTER TABLE measurements ALTER COLUMN objectid DROP NOT NULL;
DELETE FROM measurements WHERE objectid IS NULL;

WITH m AS (
  SELECT
    (CASE
      WHEN m.distance_sum = 0
      THEN intermediate_point(m.g0, m.g1, m.distance, 0.5)
      ELSE intermediate_point(m.g0, m.g1, m.distance, m.distance_delta_sum/m.distance_sum)
    END) AS geom,
    m.time, m.speed, m.co2, m.consumption, m.sensor, m.track
  FROM (
    SELECT
      id,
      SUM(m.distance_delta) OVER points_up_to_here AS distance_delta_sum,
      SUM(m.distance_delta) OVER all_points +
        LAST_VALUE(m.speed) OVER all_points *
        (m.time_1 - LAST_VALUE(m.time) OVER all_points) AS distance_sum,
      epoch_to_timestamp(m.time) AS time,
      m.speed*3.6 AS speed,
      m.co2,
      m.consumption,
      m.sensor,
      m.track,
      m.distance,
      m.g0,
      m.g1
    FROM (
      SELECT m.*,
        (LAG(m.speed, 1, m.speed_0) OVER w) *
          (m.time-(LAG(m.time, 1, m.time_0) OVER w)) AS distance_delta
      FROM (
        SELECT m.id, m.distance, m.g0, m.g1, m.speed_0,
          interpolate_linear(m.speed_0, m.speed_1, m.i, m.n) AS speed,
          m.time_0, m.time_1,
          interpolate_linear(m.time_0, m.time_1, m.i, m.n) AS time,
          interpolate_linear(m.co2_0, m.co2_1, m.i, m.n) AS co2,
          interpolate_linear(m.consumption_0, m.consumption_1, m.i, m.n) AS consumption,
          m.sensor,
          m.track
        FROM (
          SELECT
            m.id,
            LAG(m.geom) OVER w AS g0,
            m.geom AS g1,
            angular_distance(LAG(m.geom) OVER w, m.geom) AS distance,
            EXTRACT(EPOCH FROM LAG(m.time) OVER w) AS time_0,
            EXTRACT(EPOCH FROM m.time) AS time_1,
            (LAG(m.speed) OVER w)/3.6 AS speed_0,
            m.speed/3.6 AS speed_1,
            LAG(m.co2) OVER w AS co2_0,
            m.co2 AS co2_1,
            LAG(m.consumption) OVER w AS consumption_0,
            m.consumption AS consumption_1,
            m.sensor,
            m.track,
            generate_series(1, 5) AS i,
            5 AS n
          FROM measurements AS m
          WINDOW w AS (
            PARTITION BY m.track
            ORDER BY m.time
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
          ORDER BY track, time_0, time_1
        ) AS m
      ) AS m
      WHERE m.time IS NOT NULL
      WINDOW w AS (
        PARTITION BY m.id
        ORDER BY m.time
        ROWS 1 PRECEDING)
    ) AS m
    WINDOW
      all_points AS (
        PARTITION BY m.id
        ORDER BY m.time
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
      points_up_to_here AS (
        PARTITION BY m.id
        ORDER BY m.time
        ROWS UNBOUNDED PRECEDING)
  ) AS m
)
INSERT INTO measurements (geom, time, speed, speed_unit, co2, co2_unit,
                          consumption, consumption_unit, sensor, track)
  SELECT geom, time, speed, 'km/h' AS speed_unit, co2, 'kg/h' AS co2_unit,
         consumption, 'l/h' AS consumption_unit, sensor, track
  FROM m;

INSERT INTO trajectories (track, start_time, end_time, geom)
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(   1,  1000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(1001,  2000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(2001,  3000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(3001,  4000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(4001,  5000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(5001,  6000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(6001,  7000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(7001,  8000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(8001,  9000)
  UNION ALL
  SELECT id, start_time, end_time, geom
  FROM create_trajectories(9001, 10000)
