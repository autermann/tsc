
from config import fgdb, setenv

if __name__ == '__main__':
    setenv()

    classifiers = [
        'all',
        'evening',
        'morning',
        'noon'
    ]

    tables = [
        'co2_by_axis',
        'co2_by_axis_segment',
        'consumption_by_axis',
        'consumption_by_axis_segment',
        'passages_by_axis',
        'passages_by_axis_segment',
        'stops_by_axis',
        'stops_by_axis_segment',
        'travel_time_by_axis',
        'travel_time_by_axis_segment'
    ]

    for table in tables:
        for classifier in classifiers:
            fgdb.table('%s_%s' % (table, classifier)).delete_if_exists()
