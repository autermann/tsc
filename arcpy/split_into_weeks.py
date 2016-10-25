from ooarcpy import FileGDB
from config import fgdb, setenv, workspace
from datetime import datetime, timedelta
import os

EPOCH = datetime(1970, 1, 1)

def to_millis(dt):
    return long((dt-EPOCH).total_seconds() * 1000)


if __name__ == '__main__':
  setenv()
  measurements = fgdb.feature_class('measurements').view()
  begin_period_1 = datetime(2016, 6, 6)
  begin_period_2 = datetime(2016, 9, 5)
  period_length = 4
  two_hours = timedelta(hours=2)

  times = {}
  times.update({
    'week%d' % (week + 1): [(
      begin_period_1 - two_hours + timedelta(weeks=week),
      begin_period_1 - two_hours + timedelta(weeks=week + 1)
    )] for week in range(period_length)
  })
  times.update({
    'week%d' % (week + 5): [(
      begin_period_2 - two_hours + timedelta(weeks=week),
      begin_period_2 - two_hours + timedelta(weeks=week + 1)
    )] for week in range(period_length)
  })
  times['summer'] = [(
    begin_period_1 - two_hours + timedelta(weeks=period_length),
    begin_period_2 - two_hours
  )]
  times['all'] = [
    (begin_period_1 - two_hours, begin_period_1 - two_hours + timedelta(weeks=period_length)),
    (begin_period_2 - two_hours, begin_period_2 - two_hours + timedelta(weeks=period_length))
  ]

  for name in times:
    out = FileGDB(os.path.join(workspace, '%s.gdb' % name))
    out.delete_if_exists()
    out.create_if_not_exists()

    out_fc = out.feature_class('measurements')

    where_clause = """("time" >= {begin} AND "time" < {end})"""
    where_clause = ' OR '.join(where_clause.format(begin=to_millis(t[0]), end=to_millis(t[1])) for t in times[name])
    measurements.new_selection(where_clause=where_clause)
    measurements.to_feature_class(out_fc)

    for index in measurements.list_indexes():
      fields = [field.name for field in index.fields]
      if measurements.oid_field_name not in fields and measurements.shape_field_name not in fields:
        out_fc.add_index(fields, index.name, index.isUnique, index.isAscending)
