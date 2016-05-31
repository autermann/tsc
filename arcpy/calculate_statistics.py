import arcpy
import os
import ec

if __name__ == '__main__':
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = r'C:\tsc\workspace'
    fgdb = os.path.join(arcpy.env.workspace, 'outputs.gdb')

    measurements_fc = os.path.join(fgdb, 'measurements')
    stops_table = os.path.join(fgdb, 'stops')

    ec.calculate_statistics(measurements_fc, stops_table, fgdb)

