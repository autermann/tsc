from ec import create_stop_table
from config import fgdb, setenv

if __name__ == '__main__':
    setenv()

    create_stop_table(fgdb.feature_class('measurements'),
    	              fgdb.table('stops'))

