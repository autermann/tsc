from arcpy import env
from os import path
from ec import calculate_statistics
from ooarcpy import FileGDB

if __name__ == '__main__':
    env.overwriteOutput = True
    env.workspace = r'C:\tsc\workspace'

    fgdb = FileGDB(path.join(env.workspace, 'outputs.gdb'))
    measurements = fgdb.feature_class('measurements')
    stops = fgdb.table('stops')

    calculate_statistics(measurements, stops, fgdb)

