from ec import calculate_statistics
from config import fgdb, setenv

if __name__ == '__main__':
    setenv()
    measurements = fgdb.feature_class('measurements')
    stops = fgdb.table('stops')

    calculate_statistics(measurements, stops, fgdb)

