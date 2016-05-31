import arcpy
import os
import ec

if __name__ == '__main__':
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = r'C:\tsc\workspace'
    fgdb = os.path.join(arcpy.env.workspace, 'outputs.gdb')
    ec.create_stop_table(fgdb, 'stops', os.path.join(fgdb, 'measurements'))

