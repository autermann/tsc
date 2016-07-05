from ec import create_tracks
from config import fgdb, setenv

if __name__ == '__main__':
    setenv()
    create_tracks(in_fc=fgdb.feature_class('measurements'),
                  out_fc=fgdb.feature_class('tracks'))
