from arcpy import env
from ec import AxisModel, create_axis_subsets, axis
from ooarcpy import SDE
from os import path

if __name__ == '__main__':
    basedir = r'C:\tsc'
    modeldir = path.join(basedir, 'model')

    workspace = env.workspace = path.join(basedir, 'workspace')

    env.overwriteOutput = True

    sde = SDE(path = path.join(workspace, 'envirocar.sde'),
              database = 'envirocar', hostname = 'localhost',
              username = 'postgres',  password = 'postgres')
    sde.create_if_not_exists()

    axis_model = AxisModel.for_dir(modeldir)

    create_axis_subsets(
        measurements_fc = sde.feature_class('measurements'),
        trajectories_fc = sde.feature_class('trajectories'),
        tracks_fc = sde.feature_class('tracks'),
        #axes = None,
        #axes = [axis for axis in axis(xrange(3, 4 + 1))],
        #axes = ['4_2'],
        axes = ['1_1','1_2'],
        time = None,
        out_dir = workspace,
        out_name = 'outputs.gdb',
        node_tolerance = 20,
        axis_model = axis_model)
