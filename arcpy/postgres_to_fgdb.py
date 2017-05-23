import arcpy
import os
import logging
import ooarcpy

log = logging.getLogger(__name__)

config.enviroCar.create_if_not_exists()
config.sde.create_if_not_exists()

target = config.enviroCar.feature_class('trajectories')
log.debug('Exporting trajectories')
config.sde.feature_class('trajectories').to_feature_class(target)
log.debug('Creating indices')
target.add_index(['track'], 'track_idx')
target.add_index(['end_time'], 'end_time_idx')
target.add_index(['start_time'], 'start_time_idx')


target = config.enviroCar.feature_class('measurements')
log.debug('Exporting measurements')
config.sde.feature_class('measurements').to_feature_class(target)
log.debug('Creating indices')
target.add_index(['track'], 'track_idx')
target.add_index(['time'], 'time_idx')
target.add_index(['time', 'track'], 'time_track_idx')


source = config.sde.feature_class('tracks')
target = config.enviroCar.feature_class('tracks')
log.debug('Exporting tracks')
field_mapping = """
objectid   "objectid"   true false false 24 Text   0 0, First, #, {source}, objectid,   -1, -1;
start_time "start_time" true false false 50 Double 0 0, First, #, {source}, start_time, -1, -1;
end_time   "end_time"   true false false 50 Double 0 0, First, #, {source}, end_time,   -1, -1;
track      "track"      true true  false 50 Long   0 0, First, #, {source}, id,         -1, -1
""".format(source=source.id)
source.to_feature_class(target, field_mapping=field_mapping)
log.debug('Creating indices')
target.add_index(['track'], 'track_idx')
