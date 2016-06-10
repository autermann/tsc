from arcpy import env
from os import path
from ec import create_tracks
from ooarcpy import FileGDB

if __name__ == '__main__':
    env.overwriteOutput = True
    env.workspace = r'C:\tsc\workspace'
    fgdb = FileGDB(path.join(env.workspace, 'outputs.gdb'))

    create_tracks(in_fc=fgdb.feature_class('measurements'),
                  out_fc=fgdb.feature_class('tracks'))
