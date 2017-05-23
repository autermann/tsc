import ec
import config

if __name__ == '__main__':
    config.setenv()

    ec.create_axis_subsets(
        measurements_fc = config.enviroCar.feature_class('measurements'),
        trajectories_fc = config.enviroCar.feature_class('trajectories'),
        tracks_fc = config.enviroCar.feature_class('tracks'),
        axes = config.axes,
        time = None,
        out_dir = config.workspace,
        out_name = 'outputs.gdb',
        axis_model = config.axis_model)
