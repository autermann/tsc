from ooarcpy import FileGDB
from config import fgdb, setenv, workspace
from datetime import datetime, timedelta
from utils import SQL
import os

if __name__ == '__main__':

    setenv()

    start = datetime(2016, 6, 6) - timedelta(hours=2)
    weeks = 4



    measurements = fgdb.feature_class('measurements').view()
    one_week = timedelta(weeks=1)
    where_clause = """"time" >= date '{begin!s}' AND "time" < date '{end!s}'"""

    for week in range(weeks):
        begin = start + week * one_week
        end = begin + one_week
        out = FileGDB(os.path.join(workspace, 'week%d.gdb' % (week + 1)))
        out.create_if_not_exists()
        measurements.new_selection(where_clause.format(begin=begin, end=end))
        measurements.to_feature_class(out.feature_class('measurements'))
