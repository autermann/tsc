

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

    def is_in_range(time, min_hour, max_hour):
        time = parse(time)

        return (0 <= time.weekday() < 5 and min_hour <= time.hour < max_hour)
    """)
    fc = fgdb.feature_class('measurements')
    fc.add_field('morning', 'SHORT')
    fc.add_field('evening', 'SHORT')
    fc.add_field('noon', 'SHORT')
    fc.calculate_field('morning', 'is_in_range(!time!,  6, 10)', code_block=code_block)
    fc.calculate_field('evening', 'is_in_range(!time!, 15, 19)', code_block=code_block)
    fc.calculate_field('noon',    'is_in_range(!time!, 12, 14)', code_block=code_block)