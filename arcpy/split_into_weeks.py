from ooarcpy import FileGDB
from config import fgdb, setenv, workspace
from datetime import datetime, timedelta
from utils import SQL
import os


setenv()

measurements = fgdb.feature_class('measurements').view()

times = {}
times.update({
  'week%d' % (week + 1): [(
    datetime(2016, 6, 6) - timedelta(hours=2) + timedelta(weeks=week),
    datetime(2016, 6, 6) - timedelta(hours=2) + timedelta(weeks=week + 1)
  )] for week in range(4)
})
times.update({
  'week%d' % (week + 5): [(
    datetime(2016, 9, 5) - timedelta(hours=2) + timedelta(weeks=week),
    datetime(2016, 9, 5) - timedelta(hours=2) + timedelta(weeks=week + 1)
  )] for week in range(4)
})
times['summer'] = [(
  datetime(2016, 6, 6) - timedelta(hours=2) + timedelta(weeks=4),
  datetime(2016, 9, 5) - timedelta(hours=2)
)]
times['all'] = [
  (datetime(2016, 6, 6) - timedelta(hours=2), datetime(2016, 6, 6) - timedelta(hours=2) + timedelta(weeks=4)),
  (datetime(2016, 9, 5) - timedelta(hours=2), datetime(2016, 9, 5) - timedelta(hours=2) + timedelta(weeks=4))
]

for name in times:
  out = FileGDB(os.path.join(workspace, '%s.gdb' % name))
  out.create_if_not_exists()

  out_fc = out.feature_class('measurements')

  where_clause = """("time" >= date '{begin!s}' AND "time" < date '{end!s}')"""
  where_clause = ' OR '.join(where_clause.format(begin=t[0], end=t[1]) for t in times[name])
  measurements.new_selection(where_clause=where_clause)
  measurements.to_feature_class(out_fc)

  for index in measurements.list_indexes():
    fields = [field.name for field in index.fields]
    if measurements.oid_field_name not in fields and measurements.shape_field_name not in fields:
      out_fc.add_index(fields, index.name, index.isUnique, index.isAscending)