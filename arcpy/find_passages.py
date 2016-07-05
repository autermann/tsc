from ec import find_passages
from config import fgdb, axis_model, setenv

if __name__ == '__main__':
    setenv()
    measurements_fc = fgdb.feature_class('measurements')
    find_passages(fgdb, axis_model, measurements_fc)