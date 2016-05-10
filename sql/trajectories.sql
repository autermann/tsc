DROP TABLE IF EXISTS trajectories;

CREATE TABLE trajectories (
  id SERIAL PRIMARY KEY,
  track INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
  start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  geom GEOMETRY(LineString, 4326) NOT NULL
);

INSERT INTO trajectories (track, start_time, end_time, geom)
  SELECT track, start_time, end_time, geom
  FROM (
    SELECT track, rank, geom,
      (
        CASE
          WHEN ROW_NUMBER() OVER w = 2
          THEN
            CASE
              WHEN GeometryType(LAG(geom) OVER w) = 'POINT'
              THEN LAG(start_time) OVER w
              ELSE start_time
            END
          ELSE start_time
        END
      ) AS start_time,
      (
        CASE
          WHEN GeometryType(LEAD(geom) OVER w) = 'POINT'
          THEN LEAD(end_time) OVER w
          ELSE end_time
        END
      ) AS end_time
    FROM (
      SELECT
        track,
        ROW_NUMBER() OVER (PARTITION BY track ORDER BY MIN(rank)) AS rank,
        MIN(start_time) AS start_time,
        MAX(end_time) AS end_time,
        MIN(geom) AS geom
      FROM (
        SELECT *, SUM(r) OVER (ORDER BY rank) AS grp
        FROM (
          SELECT *,
            (
              CASE
                WHEN ST_OrderingEquals(geom, LAG(geom) OVER w)
                THEN NULL ELSE 1
              END
             ) AS r
          FROM (
            SELECT track, rank, start_time, end_time,
              (CASE
                WHEN ST_NumPoints(geom) = 2
                  AND ST_Equals(ST_StartPoint(geom), ST_Endpoint(geom))
                THEN ST_StartPoint(geom)
                ELSE geom
              END) AS geom
            FROM (
              SELECT track,
                ROW_NUMBER() OVER w AS rank,
                LAG(time) OVER w AS start_time,
                time AS end_time,
                ST_RemoveRepeatedPoints(
                  ST_SetSRID(ST_MakeLine(LAG(geom) OVER w, geom), 4326)
                ) AS geom
              FROM measurements
              WINDOW w AS (PARTITION BY track ORDER BY time)
              ORDER BY track, time
              OFFSET 1
            ) AS foo
          ) AS foo
          WINDOW w AS (PARTITION BY track ORDER BY rank)
        ) AS foo
      ) AS foo
      GROUP BY track, grp
      ORDER BY track, rank
    ) AS foo
    WINDOW w AS (PARTITION BY track ORDER BY rank ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    ORDER BY track, rank
  ) AS foo
  WHERE GeometryType(geom) = 'LINESTRING'
  ORDER BY track, rank;

CREATE INDEX ON trajectories (track);
CREATE INDEX ON trajectories USING GIST (geom) WITH (FILLFACTOR = 100);
CREATE INDEX ON trajectories (start_time);
CREATE INDEX ON trajectories (end_time);