ALTER TABLE measurements ADD COLUMN sensor_id CHAR(24);
ALTER TABLE measurements ADD COLUMN track_id CHAR(24);

UPDATE measurements AS m 
  SET sensor_id = (SELECT t.objectid FROM sensors AS t WHERE t.id = m.sensor),
      track_id = (SELECT t.objectid FROM tracks AS t WHERE t.id = m.track);

ALTER TABLE measurements DROP COLUMN sensor;
ALTER TABLE measurements RENAME COLUMN sensor_id TO sensor;
ALTER TABLE measurements ALTER COLUMN sensor SET NOT NULL;
ALTER TABLE measurements DROP COLUMN track;
ALTER TABLE measurements RENAME COLUMN track_id TO track;
ALTER TABLE measurements ALTER COLUMN track SET NOT NULL;

DROP TABLE IF EXISTS trajectories;
DROP TABLE IF EXISTS sensors;
DROP TABLE IF EXISTS tracks;