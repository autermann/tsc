import arcpy
import os

tracks_in=r'Database Connections\envirocar@localhost.sde\envirocar.public.tracks'
measurements_in=r'Database Connections\envirocar@localhost.sde\envirocar.public.measurements'
trajectories_in=r'Database Connections\envirocar@localhost.sde\envirocar.public.trajectories'

out_fgdb=r'C:\tsc\envirocar_new.gdb'

arcpy.conversion.FeatureClassToFeatureClass(in_features=trajectories_in, out_path=out_fgdb, out_name='trajectories')
arcpy.conversion.FeatureClassToFeatureClass(in_features=measurements_in, out_path=out_fgdb, out_name='measurements')
arcpy.conversion.FeatureClassToFeatureClass(in_features=tracks_in,       out_path=out_fgdb, out_name='tracks',
	field_mapping="""
	objectid "objectid" true false false 24 Text 0 0 ,First,#,{tracks_in},objectid,-1,-1;
	start_time "start_time" true false false 36 Date 0 0 ,First,#,{tracks_in},start_time,-1,-1;
	end_time "end_time" true false false 36 Date 0 0 ,First,#,{tracks_in},end_time,-1,-1;
	track "track" true true false 50 Long 0 0 ,First,#,{tracks_in},id,-1,-1
	""".format(tracks_in=tracks_in))

arcpy.management.AddIndex(in_table=os.path.join(out_fgdb, 'measurements'), fields=['track'], index_name='track_idx')
arcpy.management.AddIndex(in_table=os.path.join(out_fgdb, 'measurements'), fields=['time'], index_name='time_idx')
arcpy.management.AddIndex(in_table=os.path.join(out_fgdb, 'measurements'), fields=['time', 'track'], index_name='time_track_idx')
arcpy.management.AddIndex(in_table=os.path.join(out_fgdb, 'tracks'),       fields=['track'], index_name='track_idx')
arcpy.management.AddIndex(in_table=os.path.join(out_fgdb, 'trajectories'), fields=['track'], index_name='track_idx')
arcpy.management.AddIndex(in_table=os.path.join(out_fgdb, 'trajectories'), fields=['end_time'], index_name='end_time_idx')
arcpy.management.AddIndex(in_table=os.path.join(out_fgdb, 'trajectories'), fields=['start_time'], index_name='start_time_idx')