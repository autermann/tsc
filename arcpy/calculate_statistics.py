from ec import calculate_statistics, create_tracks, create_stop_table, find_passages
from config import fgdb, setenv, axis_model

if __name__ == '__main__':
    setenv()
    measurements = fgdb.feature_class('measurements')
    stops = fgdb.table('stops')
    tracks = fgdb.feature_class('tracks')

    create_tracks(measurements, tracks)
    create_stop_table(measurements, stops)
    calculate_statistics(fgdb)
    find_passages(fgdb, axis_model)

# consumption_by_axis_all: axis|num_observations|consumption
# consumption_by_axis_morning: axis|num_observations|consumption
# consumption_by_axis_evening: axis|num_observations|consumption
# consumption_by_axis_noon: axis|num_observations|consumption
# co2_by_axis_all: axis|num_observations|co2
# co2_by_axis_morning: axis|num_observations|co2
# co2_by_axis_evening: axis|num_observations|co2
# co2_by_axis_noon: axis|num_observations|co2
# consumption_by_axis_segment_all: axis|segment|num_observations|consumption|join_field
# consumption_by_axis_segment_morning:_ axis|segment|num_observations|consumption|join_field
# consumption_by_axis_segment_evening: axis|segment|num_observations|consumption|join_field
# consumption_by_axis_segment_noon: axis|segment|num_observations|consumption|join_field
# co2_axis_segment: axis|segment|num_observations|co2|join_field
# co2_axis_segment_morning: axis|segment|num_observations|co2|join_field
# co2_axis_segment_evening: axis|segment|num_observations|co2|join_field
# co2_axis_segment_noon: axis|segment|num_observations|co2|join_field
# stops_by_axis_all: axis|stops|duration
# stops_by_axis_morning: axis|stops|duration
# stops_by_axis_evening: axis|stops|duration
# stops_by_axis_noon: axis|stops|duration
# stops_by_axis_segment_all: axis|segment|stops|duration
# stops_by_axis_segment_morning: axis|segment|stops|duration
# stops_by_axis_segment_evening: axis|segment|stops|duration
# stops_by_axis_segment_noon: axis|segment|stops|duration
# travel_time_by_axis_all: axis|num_tracks|travel_time
# travel_time_by_axis_morning: axis|num_tracks|travel_time
# travel_time_by_axis_evening: axis|num_tracks|travel_time
# travel_time_by_axis_noon: axis|num_tracks|travel_time
# travel_time_by_axis_segment_all: axis|segment|num_tracks|travel_time|join_field
# travel_time_by_axis_segment_morning: axis|segment|num_tracks|travel_time|join_field
# travel_time_by_axis_segment_evening: axis|segment|num_tracks|travel_time|join_field
# travel_time_by_axis_segment_noon: axis|segment|num_tracks|travel_time|join_field
# passages_by_axis_segment_track_all: axis|segment|passages_with_stops|passages_without_stops|passages_overall|join_field
# passages_by_axis_segment_track_morning: axis|segment|passages_with_stops|passages_without_stops|passages_overall|join_field
# passages_by_axis_segment_track_evening: axis|segment|passages_with_stops|passages_without_stops|passages_overall|join_field
# passages_by_axis_segment_track_noon: axis|segment|passages_with_stops|passages_without_stops|passages_overall|join_field