import os
import arcpy
import ooarcpy
import config


if __name__ == '__main__':
    if not os.path.exists(config.workspace_cest):
        os.makedirs(config.workspace_cest)
    
    for name in config.names:
        source = os.path.join(config.workspace, '%s.gdb' % name)
        target = os.path.join(config.workspace_cest,'%s.gdb' % name)
        
        if arcpy.Exists(target):
            arcpy.management.Delete(target)
        
        arcpy.management.Copy(source, target)
        target = ooarcpy.FileGDB(target)
        
        measurements = target.feature_class('measurements')
        measurements.calculate_field('time', '!time!+7200000')
        
        tracks = target.feature_class('tracks')
        tracks.calculate_field('start_time', '!start_time!+7200000')
        tracks.calculate_field('stop_time', '!stop_time!+7200000')
