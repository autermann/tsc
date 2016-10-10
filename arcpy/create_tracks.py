
from config import fgdb as fgdb_all, setenv, axis_model, workspace
from ec import create_tracks
from ooarcpy import FileGDB
import os

if __name__ == '__main__':
  setenv()
  #fgdbs = [fgdb_all]
  fgdbs = [fgdb_all] + [FileGDB(os.path.join(workspace, 'week%d.gdb' % (week + 1))) for week in xrange(4)]
  #fgdbs = [FileGDB(os.path.join(workspace, 'week%d.gdb' % (week + 1))) for week in xrange(4)]
  #fgdbs = [FileGDB(os.path.join(workspace, 'week1.gdb'))]

  for fgdb in fgdbs:
    measurements = fgdb.feature_class('measurements')
    tracks = fgdb.feature_class('tracks')
    create_tracks(measurements, tracks)

