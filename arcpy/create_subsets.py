from ec import create_axis_subsets
from config import sde, axis_model, workspace, axes, setenv

if __name__ == '__main__':
    setenv()

    sde.create_if_not_exists()

    create_axis_subsets(
        measurements_fc = sde.feature_class('measurements'),
        trajectories_fc = sde.feature_class('trajectories'),
        tracks_fc = sde.feature_class('tracks'),
        axes = axes,
        time = None,
        out_dir = workspace,
        out_name = 'outputs.gdb',
        axis_model = axis_model)
