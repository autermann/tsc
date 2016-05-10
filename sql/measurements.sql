CREATE INDEX ON measurements (track);
CREATE INDEX ON measurements (sensor);
CREATE INDEX ON measurements (time);
CREATE INDEX ON measurements USING GIST(geom) WITH (FILLFACTOR = 100);