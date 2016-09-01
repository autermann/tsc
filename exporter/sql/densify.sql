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


--DROP FUNCTION intermediate_point(GEOMETRY(Point, 4326), GEOMETRY(Point, 4326), DOUBLE PRECISION);
--DROP FUNCTION intermediate_point(GEOMETRY(Point, 4326), GEOMETRY(Point, 4326), DOUBLE PRECISION, DOUBLE PRECISION);
--DROP FUNCTION angular_distance(GEOMETRY(Point, 4326), GEOMETRY(Point, 4326));
--DROP FUNCTION interpolate_linear(DOUBLE PRECISION, DOUBLE PRECISION, INTEGER, INTEGER);
--DROP FUNCTION epoch_to_timestamp(DOUBLE PRECISION);