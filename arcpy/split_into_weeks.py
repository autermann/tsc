import os
import ooarcpy
import config

from datetime import datetime, timedelta

EPOCH = datetime(1970, 1, 1)

def to_millis(dt):
    return long((dt-EPOCH).total_seconds() * 1000)


if __name__ == '__main__':
  config.setenv()
  measurements = config.fgdb.feature_class('measurements').view()
  begin_period_1 = datetime(2016, 6, 6)
  begin_period_2 = datetime(2016, 9, 5)
  period_length = 4
  two_hours = timedelta(hours=2)

  times = {}
  times['period1'] = [
    (datetime(2016, 6,  6) - two_hours, datetime(2016,  6, 19) - two_hours),
    (datetime(2016, 6, 27) - two_hours, datetime(2016,  7,  3) - two_hours),
    (datetime(2016, 9,  5) - two_hours, datetime(2016,  9, 18) - two_hours),
    (datetime(2016, 9, 26) - two_hours, datetime(2016, 10,  2) - two_hours)
  ]
  times['period2'] = [
    (datetime(2016, 6, 20) - two_hours, datetime(2016,  6, 26) - two_hours),
    (datetime(2016, 9, 19) - two_hours, datetime(2016,  9, 23) - two_hours)
  ]
  times['period3'] = [
    (datetime(2016, 7, 11) - two_hours, datetime(2016,  8, 23) - two_hours),
  ]

  for name in times:
    fgdb = ooarcpy.FileGDB(os.path.join(config.workspace, '%s.gdb' % name))
    fgdb.delete_if_exists()
    fgdb.create_if_not_exists()

    out = fgdb.feature_class('measurements')

    # select the subset matching the times
    where_clause = ' OR '.join("""("time" >= {begin} AND "time" < {end})""".format(
                begin=to_millis(t[0]), end=to_millis(t[1])) for t in times[name])
    measurements.new_selection(where_clause=where_clause)
    # and copy it to a new feature class
    measurements.to_feature_class(out)

    # copy the indices
    for index in measurements.list_indexes():
      fields = [field.name for field in index.fields]
      if measurements.oid_field_name not in fields and measurements.shape_field_name not in fields:
        out.add_index(fields, index.name, index.isUnique, index.isAscending)
