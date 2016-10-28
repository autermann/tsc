import os
import arcpy
import ooarcpy
import config


if __name__ == '__main__':
    directory = os.path.join(config.workspace, 'cest')
    if not os.path.exists(directory):
        os.makedirs(directory)
    names = ['summer.gdb', 'all.gdb'] + ['week%d.gdb' % (week+1) for week in range(8)]
    for name in names:
        source = os.path.join(config.workspace, name)
        target = os.path.join(directory, name)
        if arcpy.Exists(target):
            arcpy.management.Delete(target)
        arcpy.management.Copy(source, target)
        fgdb = ooarcpy.FileGDB(target)
        measurements = fgdb.feature_class('measurements')
        measurements.calculate_field('time', '!time!+7200000')
        tracks = fgdb.feature_class('tracks')
        tracks.calculate_field('start_time', '!start_time!+7200000')
        tracks.calculate_field('stop_time', '!stop_time!+7200000')
