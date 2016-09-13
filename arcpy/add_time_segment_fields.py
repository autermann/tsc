

from config import setenv, fgdb
from ec import add_time_segment_fields


if __name__ == '__main__':
    setenv()
    add_time_segment_fields(fgdb.feature_class('measurements'))