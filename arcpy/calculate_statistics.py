import config
import ec
import ooarcpy
import os
import logging

log = logging.getLogger(__name__)

if __name__ == '__main__':
    config.setenv()

    for fgdb in config.fgdbs:

        log.debug('calculating statistics for %s', fgdb.id)
        measurements = fgdb.feature_class('measurements')
        stops = fgdb.table('stops')
        try:
            tracks = fgdb.feature_class('tracks')
            
            log.debug('creating tracks')
            ec.create_tracks(measurements, tracks)
            
            log.debug('creating stop table')
            ec.create_stop_table(measurements, stops)
            
            log.debug('calculating statistics')
            ec.calculate_statistics(config.axis_model, fgdb)
            
            log.debug('finding passages')
            ec.find_passages(fgdb, config.axis_model)
        finally:
            stops.delete_if_exists()

        log.debug('creating result tables')
        ec.create_result_tables(fgdb, config.axis_model)
