import arcpy
import os
import ec

if __name__ == '__main__':
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = r'C:\tsc\workspace'
    fgdb = os.path.join(arcpy.env.workspace, 'outputs.gdb')
    fc = os.path.join(fgdb, 'measurements')
    ec.create_tracks(in_fc=fc, out_fc_location=fgdb, out_fc_name='tracks')
