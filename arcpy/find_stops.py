from arcpy import env
from os import path
from ec import create_stop_table
from ooarcpy import FileGDB

if __name__ == '__main__':
    env.overwriteOutput = True
    env.workspace = r'C:\tsc\workspace'
    fgdb = FileGDB(path.join(env.workspace, 'outputs.gdb'))

    create_stop_table(fgdb.feature_class('measurements'), fgdb.table('stops'))

