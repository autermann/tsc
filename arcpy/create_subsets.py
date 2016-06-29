import arcpy
import os
import ec

if __name__ == '__main__':
    basedir = r'C:\tsc'
    arcpy.env.workspace = os.path.join(basedir, 'workspace')
    arcpy.env.overwriteOutput = True

    db_params = ec.DatabaseParams(
        database = 'envirocar',
        hostname = 'localhost',
        username = 'postgres',
        password = 'postgres')

    sde = db_params.create_sde(arcpy.env.workspace, 'envirocar')

    ec.create_axis_subsets(
        measurements_fc = db_params.get_feature_class('measurements'),
        trajectories_fc = db_params.get_feature_class('trajectories'),
        tracks_fc = db_params.get_feature_class('tracks'),
        axes = None,
        #axes = [axis for axis in ec.axis(xrange(3, 4 + 1))],
        #axes = ['4_2'],
        #axes = ['3_1'],
        time = None,
        out_dir = arcpy.env.workspace,
        out_name = 'outputs.gdb',
        node_tolerance = 20,
        axis_model = ec.AxisModel.for_dir(os.path.join(basedir, 'model')))
