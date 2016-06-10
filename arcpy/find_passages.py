from arcpy import env
from os import path
from ec import AxisModel, find_passages
from ooarcpy import FileGDB

if __name__ == '__main__':
    basedir = r'C:\tsc'
    env.overwriteOutput = True
    env.workspace = path.join(basedir, 'workspace')
    fgdb = FileGDB(path.join(env.workspace, 'outputs.gdb'))
    axis_model = AxisModel.for_dir(path.join(basedir, 'model'))

    measurements_fc = fgdb.feature_class('measurements')

    find_passages(fgdb, axis_model, measurements_fc)