from config import fgdb as fgdb_all, setenv, axis_model, workspace
from ec import calculate_statistics, create_tracks, create_stop_table, find_passages, create_result_tables
from ooarcpy import FileGDB
import os
import logging

log = logging.getLogger(__name__)


if __name__ == '__main__':
    setenv()


    #names = ['all', 'summer'] + ['week%d' % (week+1) for week in range(8)]
    names = ['all']
    fgdbs = [FileGDB(os.path.join(workspace, '%s.gdb' % name)) for name in names]

    for fgdb in fgdbs:

        log.debug('calculating statistics for %s', fgdb.id)
        measurements = fgdb.feature_class('measurements')
        stops = fgdb.table('stops')
        try:
            tracks = fgdb.feature_class('tracks')
            log.debug('creating tracks')
            create_tracks(measurements, tracks)
            log.debug('creating stop table')
            create_stop_table(measurements, stops)
            log.debug('calculating statistics')
            calculate_statistics(axis_model, fgdb)
            log.debug('finding passages')
            find_passages(fgdb, axis_model)
        finally:
            stops.delete_if_exists()
        log.debug('creating result tables')
        create_result_tables(fgdb, axis_model)
