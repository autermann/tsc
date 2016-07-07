from ec import calculate_statistics, create_tracks, create_stop_table, find_passages
from config import fgdb, setenv, axis_model

if __name__ == '__main__':
    setenv()
    measurements = fgdb.feature_class('measurements')
    stops = fgdb.table('stops')
    tracks = fgdb.feature_class('tracks')

    create_tracks(measurements, tracks)
    create_stop_table(measurements, stops)
    calculate_statistics(fgdb)
    find_passages(fgdb, axis_model)
