
from config import fgdb as fgdb_all, setenv, axis_model, workspace
from ec import create_tracks
from ooarcpy import FileGDB
import os

if __name__ == '__main__':
  setenv()

  names = ['summer', 'all'] + ['week%d' % (week+1) for week in range(8)]
  fgdbs = [FileGDB(os.path.join(workspace, '%s.gdb' % name)) for name in names]

  for fgdb in fgdbs:
    measurements = fgdb.feature_class('measurements')
    tracks = fgdb.feature_class('tracks')
    create_tracks(measurements, tracks)
