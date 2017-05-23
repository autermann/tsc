import ooarcpy
import os
import config
from datetime import datetime, timedelta

def to_millis(dt):
    return long((dt-datetime(1970, 1, 1)).total_seconds() * 1000)

def copy_indices(source, target):
	for index in source.list_indexes():
		fields = [field.name for field in index.fields]
		if source.oid_field_name not in fields and source.shape_field_name not in fields:
			target.add_index(fields, index.name, index.isUnique, index.isAscending)


fgdb = ooarcpy.FileGDB(os.path.join(config.workspace, 'envirocar_subset.gdb'))
fgdb.delete_if_exists()
fgdb.create_if_not_exists()

begin = to_millis(datetime(2016, 9, 20) - timedelta(hours=2))
end = to_millis(datetime(2016,  9, 21) - timedelta(hours=2))

view = config.enviroCar.feature_class('measurements').view()
out = fgdb.feature_class('measurements')
try:
	view.new_selection(where_clause="""("time" >= {begin} AND "time" < {end})""".format(begin=begin, end=end))
	view.to_feature_class(out)
	copy_indices(view, out)
finally:
	view.delete_if_exists()

view = config.enviroCar.feature_class('trajectories').view()
out = fgdb.feature_class('trajectories')
try:
	view.new_selection(where_clause="""("start_time" >= {begin} AND "end_time" < {end})""".format(begin=begin, end=end))
	view.to_feature_class(out)
	copy_indices(view, out)
finally:
	view.delete_if_exists()

view = config.enviroCar.feature_class('tracks').view()
out = fgdb.feature_class('tracks')
try:
	view.new_selection(where_clause="""("start_time" >= {begin} AND "end_time" < {end})""".format(begin=begin, end=end))
	view.to_feature_class(out)
	copy_indices(view, out)
finally:
	view.delete_if_exists()
