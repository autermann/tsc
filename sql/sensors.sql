DROP TABLE IF EXISTS sensors;

CREATE TABLE sensors (
  id SERIAL PRIMARY KEY,
  objectid CHAR(24) UNIQUE NOT NULL
);

-- fill the sensors table
INSERT INTO sensors (objectid)
  SELECT DISTINCT sensor
  FROM measurements
  ORDER BY sensor;

-- replace the sensor id with an reference
ALTER TABLE measurements ADD COLUMN sensor_id INTEGER;
UPDATE measurements AS m
  SET sensor_id = (SELECT s.id FROM sensors AS s WHERE s.objectid = m.sensor);
ALTER TABLE measurements DROP COLUMN sensor;
ALTER TABLE measurements RENAME COLUMN sensor_id TO sensor;
ALTER TABLE measurements ADD FOREIGN KEY (sensor) REFERENCES sensors (id) ON DELETE CASCADE;
ALTER TABLE measurements ALTER COLUMN sensor SET NOT NULL;

-- recreate the index
CREATE INDEX ON measurements (sensor);