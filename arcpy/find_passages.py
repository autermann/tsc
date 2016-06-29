import arcpy
import os
import ec

if __name__ == '__main__':
    basedir = r'C:\tsc'
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = os.path.join(basedir, 'workspace')
    fgdb = os.path.join(arcpy.env.workspace, 'outputs.gdb')
    axis_model = ec.AxisModel.for_dir(os.path.join(basedir, 'model'))
    measurements_fc = os.path.join(fgdb, 'measurements')

    ec.find_passages(fgdb, axis_model, measurements_fc)