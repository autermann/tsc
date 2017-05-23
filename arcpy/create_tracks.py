
import config
import ooarcpy
import os
import ec

if __name__ == '__main__':
  config.setenv()

  for fgdb in config.fgdbs:
    measurements = fgdb.feature_class('measurements')
    tracks = fgdb.feature_class('tracks')
    ec.create_tracks(measurements, tracks)
