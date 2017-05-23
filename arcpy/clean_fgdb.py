import config

if __name__ == '__main__':
    config.setenv()

    classifiers = [
        'all',
        'weekend_evening',
        'weekend_morning',
        'weekend_noon',
        'weekend_night',
        'evening',
        'morning',
        'noon',
        'night',
        'weekday_evening',
        'weekday_morning',
        'weekday_noon',
        'weekday_night',
        'workday_evening',
        'workday_morning',
        'workday_noon',
        'workday_night'
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
            config.fgdb.table('%s_%s' % (table, classifier)).delete_if_exists()
