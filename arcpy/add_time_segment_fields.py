

import textwrap
from config import setenv, fgdb

if __name__ == '__main__':
    code_block = textwrap.dedent("""\
    from datetime import datetime

    def parse(s):
        format_string = None
        if len(s) > 19:
            format_string = '%d.%m.%Y %H:%M:%S.%f'
        elif len(s) > 10:
            format_string = '%d.%m.%Y %H:%M:%S'
        else:
            format_string = '%d.%m.%Y'
        return datetime.strptime(s, format_string)

        time = parse(time)
        if min_hour <= max_hour:
            return (time.weekday() < 5 and min_hour <= time.hour < max_hour)
        else:
            return (time.weekday() < 5 and (min_hour <= time.hour or time.hour < max_hour))

    def weekend_is_in_range(time, min_hour, max_hour):
        time = parse(time)
        if min_hour <= max_hour:
            return (time.weekday() >= 5 and min_hour <= time.hour < max_hour)
        else:
            return (time.weekday() >= 5 and (min_hour <= time.hour or time.hour < max_hour))
    """)
    fc = fgdb.feature_class('measurements')
    fc.add_field('weekend_morning', 'SHORT')
    fc.add_field('weekend_noon', 'SHORT')
    fc.add_field('weekend_evening', 'SHORT')
    fc.add_field('weekend_night', 'SHORT')



    fc.calculate_field('weekend_morning', 'weekend_is_in_range(!time!,  4, 8)', code_block=code_block)
    fc.calculate_field('weekend_noon',    'weekend_is_in_range(!time!, 10, 12)', code_block=code_block)
    fc.calculate_field('weekend_evening', 'weekend_is_in_range(!time!, 13, 17)', code_block=code_block)
    fc.calculate_field('weekend_night',   'weekend_is_in_range(!time!, 19, 4)', code_block=code_block)