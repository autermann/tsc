from config import fgdb as fgdb_all, setenv, axis_model, workspace
from ec import calculate_statistics, create_tracks, create_stop_table, find_passages
from ooarcpy import FileGDB
import os

if __name__ == '__main__':
    setenv()

    fgdbs = [fgdb_all] + [FileGDB(os.path.join(workspace, 'week%d.gdb' % (week + 1))) for week in xrange(4)]

    for fgdb in fgdbs:
        measurements = fgdb.feature_class('measurements')
        stops = fgdb.table('stops')
        try:
            tracks = fgdb.feature_class('tracks')
            create_tracks(measurements, tracks)
            create_stop_table(measurements, stops)
            calculate_statistics(axis_model, fgdb)
            find_passages(fgdb, axis_model)
        finally:
            stops.delete_if_exists()
