
from config import fgdb as fgdb_all, axis_model as model, setenv, workspace
from ec import create_result_tables
from ooarcpy import FileGDB
import os

if __name__ == '__main__':
    setenv()

    fgdbs = [fgdb_all] + [FileGDB(os.path.join(workspace, 'week%d.gdb' % (week + 1))) for week in xrange(4)]

    for fgdb in fgdbs:
        create_result_tables(fgdb, model)