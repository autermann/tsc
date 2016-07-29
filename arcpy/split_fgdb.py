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
        out_fc = out.feature_class('measurements')
        measurements.new_selection(where_clause.format(begin=begin, end=end))
        measurements.to_feature_class(out_fc)

        out_fc.add_index(['segment'], 'segment_idx')
        out_fc.add_index(['axis'], 'axis_idx')
        out_fc.add_index(['track'], 'track_idx')
        out_fc.add_index(['time'], 'time_idx')
        out_fc.add_index(['workday_morning'], 'workday_morning_idx')
        out_fc.add_index(['workday_noon'], 'workday_noon_idx')
        out_fc.add_index(['workday_evening'], 'workday_evening_idx')
        out_fc.add_index(['workday_night'], 'workday_night_idx')
        out_fc.add_index(['weekend_morning'], 'weekend_morning_idx')
        out_fc.add_index(['weekend_noon'], 'weekend_noon_idx')
        out_fc.add_index(['weekend_evening'], 'weekend_evening_idx')
        out_fc.add_index(['weekend_night'], 'weekend_night_idx')
        out_fc.add_index(['complete_axis_match'], 'complete_axis_match_idx')