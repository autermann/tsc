import arcpy
import os
import ec

if __name__ == '__main__':
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = r'C:\tsc\workspace'
    fgdb = os.path.join(arcpy.env.workspace, 'outputs.gdb')

    target = os.path.join(fgdb, 'measurements')
    subsets = [os.path.join(fgdb, 'ec_subset_for_axis_{}'.format(axis)) for axis in ec.axis(xrange(1, 19+1))]
    ec.merge_feature_classes(subsets, target)