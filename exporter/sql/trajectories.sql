DROP TABLE IF EXISTS trajectories;

CREATE TABLE trajectories (
  id SERIAL PRIMARY KEY,
  track INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
  start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  geom GEOMETRY(LineString, 4326) NOT NULL
);

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
    WINDOW w AS (PARTITION BY track ORDER BY time)
    ORDER BY track, time
    OFFSET 1
  ),
  t2 AS (
    -- transform lines with equal start and end
    -- point to single point geometries
    SELECT track, rank, start_time, end_time,
      (CASE
        WHEN ST_NumPoints(geom) = 2
          AND ST_Equals(ST_StartPoint(geom), ST_Endpoint(geom))
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
    WINDOW w AS (PARTITION BY track ORDER BY rank ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
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






CREATE INDEX ON trajectories (track);
CREATE INDEX ON trajectories USING GIST (geom) WITH (FILLFACTOR = 100);
CREATE INDEX ON trajectories (start_time);
CREATE INDEX ON trajectories (end_time);

