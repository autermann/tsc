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


    for week in range(weeks):
        begin = start + week * timedelta(weeks=1)
        end = begin + timedelta(weeks=1)

        out = FileGDB(os.path.join(workspace, 'week%d.gdb' % (week + 1)))
        out.create_if_not_exists()
        out_fc = out.feature_class('measurements')
        where_clause = """"time" >= date '{begin!s}' AND "time" < date '{end!s}'"""
        measurements.new_selection(where_clause.format(begin=begin, end=end))
        measurements.to_feature_class(out_fc)

        for index in measurements.list_indexes():
            fields = [field.name for field in index.fields]
            if measurements.oid_field_name not in fields and measurements.shape_field_name not in fields:
                out_fc.add_index(fields, index.name, index.isUnique, index.isAscending)
