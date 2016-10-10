import os
import arcpy
import textwrap
import ooarcpy
import config


code_block = textwrap.dedent("""\
from datetime import datetime, timedelta
offset = timedelta(hours=2)
def to_cest(time):
    format_string = None
    if len(time) > 19:
        format_string = '%d.%m.%Y %H:%M:%S.%f'
    elif len(time) > 10:
        format_string = '%d.%m.%Y %H:%M:%S'
    else:
        format_string = '%d.%m.%Y'
    utc = datetime.strptime(time, format_string)
    cest = utc + offset
    return cest.strftime('%d.%m.%Y %H:%M:%S')
""")

if __name__ == '__main__':

    directory = os.path.join(config.workspace, 'cest')

    if not os.path.exists(directory):
        os.makedirs(directory)

    names = ['summer', 'all'] + ['week%d' % (week+1) for week in range(8)]
    fgdbs = [FileGDB(os.path.join(workspace, name)) for name in names]

    for name in names:
        source = os.path.join(config.workspace, name)
        target = os.path.join(directory, name)

        if arcpy.Exists(target):
            arcpy.management.Delete(target)
        arcpy.management.Copy(source, target)

        fgdb = ooarcpy.FileGDB(target)
        measurements = fgdb.feature_class('measurements')
        measurements.calculate_field('time', 'to_cest(!time!)', code_block=code_block)
        tracks = fgdb.feature_class('tracks')
        tracks.calculate_field('start_time', 'to_cest(!start_time!)', code_block=code_block)
        tracks.calculate_field('stop_time', 'to_cest(!stop_time!)', code_block=code_block)


