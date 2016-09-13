import os
import arcpy
import csv
import textwrap
from arcpy import SpatialReference, Array, Point, Polyline, FieldMappings, env
from ooarcpy import FeatureClass, FileGDB
from itertools import izip, islice, tee, groupby
from datetime import timedelta, datetime
from utils import first, nwise, min_max, SQL, gzip_file
import logging

MAX_TIME_SPAN_SAME_NODE = timedelta(seconds=10)
MAX_TIME_SPAN_CONSECUTIVE_NODES = timedelta(seconds=60)

log = logging.getLogger(__name__)

class TrackMatchingResult(object):


    def __init__(self, track, matches, node_count, num_consecutive_results):
        self.track = track
        self.axis_node_count = node_count
        self.num_consecutive_results = num_consecutive_results

        matches = self.create_node_matches(matches)
        matches = self.filter_matches(matches)
        matches = self.check_match_length(matches)
        matches = self.remove_edge_matches(matches)

        self.matches = list(matches)

    @property
    def axis_segment_count(self):
        return self.axis_node_count - 1

    def __str__(self):
        return '<track: {}, matches: {}>'.format(self.track, [str(match) for match in self.matches])

    def __len__(self):
        return len(self.matches)

    def check_match_length(self, matches):
        """
        Returns only those matches that have a length greater or equal to the
        minumum required length.
        """
        for match in matches:
            if len(match) >= self.num_consecutive_results:
                yield match


    def remove_edge_matches(self, matches):
        """
        Removes the first and/or last node match if these nodes are not the
        first and last nodes of the axis.
        """
        for match in matches:
            if not match.includes_first_node:
                match.delete_min_idx()
            if not match.includes_last_node:
                match.delete_max_idx()
            yield match

    def create_node_matches(self, matches):
        """Creates NodeMatchingResult objects for the supplied tuples."""
        for match in matches:
            idx, min_time, max_time = match
            yield NodeMatchingResult(min_time=min_time,
                max_time=max_time, min_idx=idx,
                axis_node_count=self.axis_node_count)

    def filter_matches(self, matches):
        """
        Removes matches that are going in the wrong direction and merges matches
        that are consecutive into single objects.
        """
        matches = sorted(matches, key=lambda x: x.time)
        current = None

        for a, b in nwise(matches, 2):
            # non consecutive nodes
            consecutive = b.min_idx <= a.max_idx + 1 <= b.max_idx

            # same node, but a too big time difference
            too_big_time_difference = a.idx == b.idx and (a.max_time - b.min_time) < MAX_TIME_SPAN_SAME_NODE
            # TODO consecutive nodes, but a too big time difference
            #too_big_time_difference |= consecutive and (a.max_time - b.min_time) < MAX_TIME_SPAN_CONSECUTIVE_NODES

            if not consecutive or too_big_time_difference:
                if current is not None:
                    yield current
                    current = None
            elif current is None:
                current = a.merge(b)
            else:
                current = current.merge(b)

        if current is not None:
            yield current

    def as_sql_clause(self):
        """Converts this result to a SQL clause."""
        return SQL.and_((SQL.eq_('track', self.track), SQL.or_(x.as_sql_clause() for x in self.matches)))

class NodeMatchingResult(object):
    def __init__(self, axis_node_count, min_time, max_time, min_idx, max_idx=None, details=None):
        if max_idx is None:
            max_idx = min_idx

        if min_idx > max_idx:
            raise ValueError('min_idx (%s) > max_idx (%s)' % (min_idx, max_idx))

        if min_time > max_time:
            raise ValueError('min_time (%s) > max_time (%s' % (min_time, max_time))

        self.idx = (min_idx, max_idx)
        self.time = (min_time, max_time)
        self.axis_node_count = axis_node_count

        if details is None:
            if min_idx != max_idx:
                raise Exception('details is missing')
            self.details = {min_idx: self.time}
        else:

            for idx in xrange(min_idx, max_idx + 1):
                if idx not in details:
                    raise Exception('no source for node {}; range: {}-{}'.format(idx, min_idx, max_idx))

            self.details = details

    @property
    def matches_complete_axis(self):
        return self.includes_first_node and self.includes_last_node

    @property
    def includes_first_node(self):
        return self.min_idx == 0

    @property
    def includes_last_node(self):
        return self.max_idx == (self.axis_node_count - 1)

    def __len__(self):
        return self.idx[1] - self.idx[0] + 1

    def __str__(self):
        if self.min_idx == self.max_idx:
            return '<Node: {}, {}--{}>'.format(
                self.min_idx,
                str(self.min_time),
                str(self.max_time))
        else:
            return '<Node: {}--{}, {}--{}>'.format(
                self.min_idx,
                self.max_idx,
                str(self.min_time),
                str(self.max_time))

    def delete_min_idx(self):
        if self.min_idx == self.max_idx:
            raise Exception('can not shrink single index result')
        del self.details[self.min_idx]
        self.idx = (self.min_idx + 1, self.max_idx)
        self.time = (self.details[self.min_idx][0], self.time[1])

    def delete_max_idx(self):
        if self.min_idx == self.max_idx:
            raise Exception('can not shrink single index result')
        del self.details[self.max_idx]
        self.idx = (self.min_idx, self.max_idx - 1)
        try:
            self.time = (self.time[0], self.details[self.max_idx][1])
        except:
            log.debug('Details:\n %s', self.details)

    def as_sql_clause(self):
        """
        Convert this result to a SQL-WHERE-clause that can be applied to the
        measurements table.
        """
        return SQL.is_between_('time', ['date {}'.format(SQL.quote_(time.replace(microsecond=0))) for time in self.time])

    @property
    def min_idx(self):
        """The minimum node index this result represents."""
        return self.idx[0]

    @property
    def max_idx(self):
        """The maximum node index this result represents."""
        return self.idx[1]

    @property
    def min_time(self):
        """The minimum time this result represents."""
        return self.time[0]

    @property
    def max_time(self):
        """The maximum time this result represents."""
        return self.time[1]

    def _merge_details(self, a, b):
        def merge(a, b): return (min(a[0], b[0]), max(a[1], b[1]))
        z = a.copy()
        for k, v in b.items():
            z[k] = v if k not in z else merge(z[k], v)
        return z

    def merge(self, other):
        """Merges this result with another result."""
        return NodeMatchingResult(
            min_time=min(self.min_time, other.min_time),
            max_time=max(self.max_time, other.max_time),
            min_idx=min(self.min_idx, other.min_idx),
            max_idx=max(self.max_idx, other.max_idx),
            details = self._merge_details(self.details, other.details),
            axis_node_count=self.axis_node_count)

def create_axis_subsets(measurements_fc, trajectories_fc, tracks_fc, axis_model,
                        out_dir = None, out_name = 'outputs.gdb', axes = None,
                        time = None, node_tolerance = 30):
    matcher = TrackMatcher(measurements_fc=measurements_fc,
        trajectories_fc=trajectories_fc, tracks_fc=tracks_fc, axes=axes,
        time=time, out_dir=out_dir, out_name=out_name,
        node_tolerance=node_tolerance, axis_model=axis_model)
    matcher.analyze()

class AxisModel(object):
    def __init__(self, segments, start_nodes,
                 influence_nodes, lsa_nodes):
        self.segments = FeatureClass(segments)
        self.start_nodes = FeatureClass(start_nodes)
        self.influence_nodes = FeatureClass(influence_nodes)
        self.lsa_nodes = FeatureClass(lsa_nodes)

    @staticmethod
    def for_dir(directory):
        return AxisModel(
            segments = os.path.join(directory, 'Achsensegmente.shp'),
            influence_nodes = os.path.join(directory, 'N_Einflussbereich.shp'),
            lsa_nodes = os.path.join(directory, 'K_LSA.shp'),
            start_nodes = os.path.join(directory, 'S_Start.shp'))

def get_all_axes(axis_model):
        def get_axes(fc, field='Achsen_ID'):
            sql_clause = ('DISTINCT', None)
            with fc.search([field], sql_clause=sql_clause) as rows:
                for row in rows:
                    yield row[0]

        feature_classes = (
            axis_model.segments,
            axis_model.start_nodes,
            axis_model.lsa_nodes,
            axis_model.influence_nodes
        )
        axes = set(axis for feature_class in feature_classes for axis in get_axes(feature_class))
        return sorted(axes)

class TrackMatcher(object):
    TYPE_START = 1
    TYPE_LSA = 2
    TYPE_INFLUENCE = 3

    def __init__(self,
                 measurements_fc,
                 trajectories_fc,
                 tracks_fc,
                 axis_model,
                 out_dir = None,
                 out_name = 'outputs.gdb',
                 axes = None,
                 time = None,
                 node_tolerance = 30):

        self.out_dir = out_dir if out_dir is not None else env.workspace
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        self.out_name = out_name
        self.fgdb = FileGDB(os.path.join(self.out_dir, self.out_name))
        self.axis_model = axis_model

        self.measurements_fc = measurements_fc
        self.measurements_fl = None
        self.trajectories_fc = trajectories_fc
        self.trajectories_fl = None
        self.tracks_fc = tracks_fc
        self.tracks_fl = None
        self.axis_segment_fl = None
        #self.axis_mbr_fc = None
        #self.axis_mbr_fl = None
        self.node_fc = None
        self.node_buffer_fc = None
        self.node_buffer_fl = None

        self.axis_ids = axes
        self.time = time
        self.node_tolerance = node_tolerance

    def analyze(self):
        if self.axis_ids is None:
            self.axis_ids = get_all_axes(self.axis_model)

        self.fgdb.create_if_not_exists()

        self.measurements_fl = self.measurements_fc.view()
        self.trajectories_fl = self.trajectories_fc.view()
        self.tracks_fl = self.tracks_fc.view()
        self.axis_segment_fl = self.axis_model.segments.view()

        # join the different node feature classes
        self.node_fc = self.create_node_feature_class()
        # create buffers around nodes
        self.node_buffer_fc = self.create_node_buffer_feature_class()
        # create the MBR for all axis
        #self.axis_mbr_fc = self.create_axis_mbr_feature_class()

        self.node_buffer_fl = self.node_buffer_fc.view()
        #self.axis_mbr_fl = self.axis_mbr_fc.view()

        try:
            target = self.fgdb.feature_class('measurements')
            subsets = [self.create_ec_subset_for_axis(axis) for axis in self.axis_ids]
            merge_feature_classes(subsets, target)
            add_time_segment_fields(target)
        finally:
            self.node_buffer_fl.delete()
            #self.axis_mbr_fl.delete()
            self.measurements_fl.delete()
            self.trajectories_fl.delete()
            self.axis_segment_fl.delete()
            self.tracks_fl.delete()
            self.node_buffer_fc.delete_if_exists()
            self.node_fc.delete_if_exists()
            #self.axis_mbr_fc.delete_if_exists()

    def create_node_buffer_feature_class(self):
        log.info('Creating node buffers with %s meters tolerance', self.node_tolerance)
        return self.node_fc.buffer(
            self.fgdb.feature_class('nodes_buffer'),
            str(self.node_tolerance) + ' Meters')

    def create_ec_subset_for_axis(self, axis):
        matches = self.get_track_matches_for_axis(axis)

        fc = self.measurements_fc
        # create the feature class
        nfc = self.fgdb.feature_class('ec_subset_for_axis_{}'.format(axis))
        nfc.delete_if_exists()
        nfc.create(geometry_type = 'POINT', spatial_reference = SpatialReference(4326))


        # get the fields to create (ignore the geometry and OID field)
        fields = [(field.name, field.type) for field in fc.list_fields() if field.type != 'OID' and field.type != 'Geometry']
        # add the fields to the feature class and change the type of track to string
        for fname, ftype in fields:
            if fname == 'objectid':
                fname = 'mongoid'
            elif fname == 'track':
                ftype = 'text'

            nfc.add_field(fname, ftype)

        nfc.add_field('complete_axis_match', 'SHORT')
        csv_path = os.path.join(self.out_dir, 'ec_subset_for_axis_{}.csv'.format(axis))

        if not matches:
            log.info('No track matches axis %s', axis)
            self.create_empty_csv_export(csv_path)
        else:
            # select the matching measurements
            self.measurements_fl.new_selection(SQL.or_(match.as_sql_clause() for match in matches))
            # export the matching measurements as CSV
            self.export_selected_measurements_to_csv(csv_path)

            # the field names to insert/request
            fnames =  ['SHAPE@XY'] + [fname for fname, ftype in fields]
            # the index of the track field
            track_idx = fnames.index('track')
            insertNames = [name if name != 'objectid' else 'mongoid' for name in fnames] + ['complete_axis_match']

            with nfc.insert(insertNames) as insert:
                # iterate over every track
                for match in matches:
                    track = str(match.track)
                    # iterate over every matching track interval
                    for idx, time in enumerate(match.matches):

                        where_clause = SQL.and_([SQL.eq_('track', track), time.as_sql_clause()])
                        new_track_name = '_'.join([track, str(idx)])
                        log.info('%s matches complete axis? %s', new_track_name, time.matches_complete_axis)
                        with fc.search(fnames, where_clause = where_clause) as rows:
                            for row in rows:
                                insert.insertRow(
                                    [column if idx != track_idx else new_track_name for idx, column in enumerate(row)] + [1 if time.matches_complete_axis else 0])

            self.add_axis_segment_association(axis, nfc)

        gzip_file(csv_path)

        return nfc

    def add_axis_segment_association(self, axis, fc):
        extracted_axis = self.fgdb.feature_class('axis_%s' % axis)
        try:
            log.info('Exporting axis %s', axis)
            # select the segments of this axis
            axis_segment_fl = self.axis_model.segments.view()
            axis_segment_fl.new_selection(SQL.eq_('Achsen_ID', SQL.quote_(axis)))


            axis_segment_fl.copy_features(extracted_axis)


            fc.near(extracted_axis)
            fc.add_field('segment', 'LONG')
            fc.add_field('axis', 'TEXT')
            ec_subset_fl = fc.view()
            extracted_axis_fl = extracted_axis.view()
            try:
                ec_subset_fl.add_join('NEAR_FID', extracted_axis_fl, extracted_axis_fl.oid_field_name)
                ec_subset_fl.calculate_field('segment', '!axis_{}.segment_id!'.format(axis))
                ec_subset_fl.calculate_field('axis', "'{}'".format(axis))
            finally:
                ec_subset_fl.delete()
                extracted_axis_fl.delete()

            fc.delete_field('NEAR_FID')
            fc.delete_field('NEAR_DIST')
        finally:
            extracted_axis.delete_if_exists()

    #def create_axis_mbr_feature_class(self):
    #    log.info('Creating MBR for axis')
    #    fc = self.fgdb.feature_class('axis_mbr')
    #    self.node_fc.minimum_bounding_geometry(fc, group_option = 'LIST', group_field = 'AXIS')
    #    return fc

    def get_tracks_for_nodes_buffer(self, axis):
        # select only the nodes of the current axis
        self.node_buffer_fl.new_selection(SQL.eq_('AXIS', SQL.quote_(axis)))
        # select all measurements instersecting with the nodes
        self.tracks_fl.new_selection_by_location(self.node_buffer_fl)
        # get the track ids of the intersecting measurements
        with self.tracks_fl.search(['track'], sql_clause = ('DISTINCT', None)) as rows:
            return sorted(set(row[0] for row in rows))


    def create_node_feature_class(self):
        # create a the new feature class

        fc = self.fgdb.feature_class('nodes')
        fc.create(geometry_type = 'POINT',
                  spatial_reference = SpatialReference(4326))
        # and add the attribute definitions
        fc.add_field('AXIS', 'TEXT')
        fc.add_field('NODE_TYPE', 'SHORT')
        fc.add_field('NODE_RANK', 'LONG')
        # lets fill in the features
        with fc.insert(['AXIS', 'NODE_TYPE', 'NODE_RANK', 'SHAPE@']) as ic:
            # the start nodes
            has_start = False
            with self.axis_model.start_nodes.search(['Achsen_ID', 'SHAPE@']) as sc:
                for row in sc:
                    ic.insertRow((row[0], TrackMatcher.TYPE_START, 0, row[1]))

            # the start nodes of ranges of influence
            with self.axis_model.influence_nodes.search(['Achsen_ID', 'SHAPE@', 'N_Rang']) as sc:
                for row in sc:
                    ic.insertRow((row[0], TrackMatcher.TYPE_INFLUENCE, 2 * (row[2] - 1) + 1, row[1]))
            # the traffic lights
            with self.axis_model.lsa_nodes.search(['Achsen_ID', 'SHAPE@', 'K_Rang']) as sc:
                for row in sc:
                    ic.insertRow((row[0], TrackMatcher.TYPE_LSA, 2 * (row[2] - 1) + 2, row[1]))
        return fc

    def get_track_matches(self, track, axis, num_consecutive_results = 4):
        log.info('checking axis %s for track %s', axis, track)
        node_count = self.get_nodes_count(axis)
        def get_node_matches(node): return self.get_node_matches(track, axis, node)
        node_matches = [match for node in xrange(0, node_count) for match in get_node_matches(node)]
        result = TrackMatchingResult(track, node_matches, node_count, num_consecutive_results)
        log.info('result for axis %s for track %s: %s', axis, track, result)
        return result

    def get_nodes_count(self, axis):
        self.node_buffer_fl.new_selection(SQL.eq_('AXIS', SQL.quote_(axis)))
        return self.node_buffer_fl.count()

    def get_node_matches(self, track, axis, node):
        self.node_buffer_fl.new_selection(SQL.and_((SQL.eq_('AXIS', SQL.quote_(axis)),
                                                    SQL.eq_('NODE_RANK', node))))
        assert self.node_buffer_fl.count() == 1
        self.trajectories_fl.new_selection(SQL.eq_('track', track))
        self.trajectories_fl.subset_selection_by_location(self.node_buffer_fl)
        count = self.trajectories_fl.count()
        #log.debug('Selected trajectories of track %s that intersect node %s of axis %s: %d', track, node, axis, count)
        if count:
            min_time = None
            max_time = None
            threshold = timedelta(seconds=20)

            fields = ['start_time', 'end_time']
            sql_clause = (None, 'ORDER BY start_time')

            with self.trajectories_fl.search(fields, sql_clause=sql_clause) as rows:
                for min, max in rows:
                    # first match
                    if min_time is None:
                        min_time, max_time = min, max
                    # if the delta is not not to big consider it a single match
                    elif (min - max_time) < threshold:
                        max_time = max
                    else:
                        yield (node, min_time, max_time)
                        min_time = max_time = None

            if min_time is not None:
                yield (node, min_time, max_time)


    def get_track_matches_for_axis(self, axis):
        tracks = self.get_tracks_for_nodes_buffer(axis)
        log.info('%s tracks found for nodes of axis %s: %s', len(tracks), axis, tracks)


        return [x for x in [self.get_track_matches(track, axis, 4) for track in tracks] if len(x) > 0]

    def create_csv_export_fields(self):
        def to_cest(x): return x + timedelta(hours = 2)
        def format_time(x): return to_cest(x).strftime('%H:%M:%S')
        def format_date(x): return to_cest(x).strftime('%d.%m.%Y')
        def identity(x): return x

        return [
            ('latitude', 'SHAPE@Y', identity),
            ('longitude', 'SHAPE@X', identity),
            ('Zeit (MESZ)', 'time', format_time),
            ('GPS Altitude(m)', 'gps_altitude', identity),
            ('Speed(km/h)', 'speed', identity),
            ('Datum', 'time', format_date),
            ('id', 'objectid', identity),
            ('CO2(kg/h)', 'co2', identity),
            ('Consumption(l/h)', 'consumption', identity),
            ('Rpm(u/min)', 'rpm', identity),
            ('GPS Speed(km/h)', 'gps_speed', identity),
            ('GPS Bearing(deg)', 'gps_bearing', identity),
            ('GPS HDOP(precision)', 'gps_hdop', identity),
            ('GPS VDOP(precision)', 'gps_vdop', identity),
            ('GPS Accuracy(%)', 'gps_accuracy', identity),
            ('GPS PDOP(precision)', 'gps_pdop', identity)
        ]

    def create_empty_csv_export(self, outfile):
        fields = self.create_csv_export_fields()
        with open(outfile, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow([f[0] for f in fields])

    def export_selected_measurements_to_csv(self, outfile):
        fields = self.create_csv_export_fields()
        def get_rows():
            return self.measurements_fl.search([f[1] for f in fields])
        def convert_row(row):
            return [f[2](x) for f, x in zip(fields, row)]

        with open(outfile, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow([f[0] for f in fields])
            with get_rows() as rows:
                for row in rows:
                    writer.writerow(convert_row(row))

class Stop(object):
    SAMPLING_RATE = timedelta(seconds=1)

    def __init__(self, axis, segment, track, start, stop, complete):
        self.track = track
        self.segment = segment
        self.axis = axis
        # extend the time by the sampling rate to
        # fix single measurement stops
        self.start = start - Stop.SAMPLING_RATE/2
        self.stop = stop + Stop.SAMPLING_RATE/2
        self.complete = complete

    def __str__(self):
        return 'Axis: {0}, Segment: {1}, Track: {2}, Duration: {3}'.format(self.axis, self.segment, self.track, self.duration)

    @property
    def duration(self):
        return self.stop - self.start

    @staticmethod
    def find(fc, stop_start_threshold=5, stop_end_threshold=10):

        fields = ['axis', 'segment', 'track', 'time', 'speed', 'complete_axis_match']
        sql_clause = (None, 'ORDER BY axis, track, time')

        ctrack = None
        csegment = None
        caxis = None
        stop_start = None
        stop_end = None
        is_stop = False
        complete = None

        def create_stop():
            return Stop(axis=caxis,
                        segment=csegment,
                        track=ctrack,
                        start=stop_start,
                        stop=stop_end,
                        complete=complete)

        with fc.search(fields, sql_clause = sql_clause) as rows:
            for axis, segment, track, time, speed, complete_axis_match in rows:

                change = caxis != axis or ctrack != track or csegment != segment
                if is_stop and change:
                    yield create_stop()
                    is_stop = False

                complete = complete_axis_match

                caxis = axis
                ctrack = track
                csegment = segment
                if is_stop:
                    if speed <= stop_end_threshold:
                        stop_end = time
                    else:
                        yield create_stop()
                        is_stop = False
                elif speed < stop_start_threshold:
                    is_stop = True
                    stop_start = stop_end = time
            if is_stop:
                yield create_stop()

def create_stop_table(in_fc, out_table):
    field_names = ['axis', 'segment', 'track', 'start_time', 'end_time', 'duration', 'complete']
    field_types = ['TEXT', 'LONG',    'TEXT',  'DATE',       'DATE',     'LONG'    , 'SHORT'   ]

    out_table.delete_if_exists()
    out_table.create()


    for field_name, field_type in zip(field_names, field_types):
        out_table.add_field(field_name, field_type)

    with out_table.insert(field_names) as insert:
        for stop in Stop.find(in_fc, stop_start_threshold=5, stop_end_threshold=10):
            duration = long(stop.duration.total_seconds() * 10**3)
            insert.insertRow((stop.axis, stop.segment, stop.track, stop.start, stop.stop, duration, stop.complete))


    code_block = textwrap.dedent("""\
    from datetime import datetime

    def parse(s):
        if len(s) > 19:
            format_string = '%d.%m.%Y %H:%M:%S.%f'
        elif len(s) > 10:
            format_string = '%d.%m.%Y %H:%M:%S'
        else:
            format_string = '%d.%m.%Y'
        return datetime.strptime(s, format_string)

    def workday_is_in_range(start, end, min_hour, max_hour):
        start = parse(start)
        end = parse(end)
        if min_hour <= max_hour:
            start = (start.weekday() < 5 and min_hour <= start.hour < max_hour)
            end = (end.weekday() < 5 and min_hour <= end.hour < max_hour)
        else:
            start = (start.weekday() < 5 and (min_hour <= start.hour or start.hour < max_hour))
            end = (end.weekday() < 5 and (min_hour <= end.hour or end.hour < max_hour))
        return start or end

    def weekend_is_in_range(start, end, min_hour, max_hour):
        start = parse(start)
        end = parse(end)
        if min_hour <= max_hour:
            start = (start.weekday() >= 5 and min_hour <= start.hour < max_hour)
            end = (end.weekday() >= 5 and min_hour <= end.hour < max_hour)
        else:
            start = (start.weekday() >= 5 and (min_hour <= start.hour or start.hour < max_hour))
            end = (end.weekday() >= 5 and (min_hour <= end.hour or end.hour < max_hour))
        return start or end
    """)
    out_table.add_field('weekend_morning', 'SHORT')
    out_table.add_field('weekend_noon', 'SHORT')
    out_table.add_field('weekend_evening', 'SHORT')
    out_table.add_field('weekend_night', 'SHORT')

    out_table.add_field('workday_morning', 'SHORT')
    out_table.add_field('workday_noon', 'SHORT')
    out_table.add_field('workday_evening', 'SHORT')
    out_table.add_field('workday_night', 'SHORT')

    # times are in UTC, we want them to be in +2
    out_table.calculate_field('workday_morning', 'workday_is_in_range(!start_time!, !end_time!,  4, 8)', code_block=code_block)
    out_table.calculate_field('workday_noon',    'workday_is_in_range(!start_time!, !end_time!, 10, 12)', code_block=code_block)
    out_table.calculate_field('workday_evening', 'workday_is_in_range(!start_time!, !end_time!, 13, 17)', code_block=code_block)
    out_table.calculate_field('workday_night',   'workday_is_in_range(!start_time!, !end_time!, 19, 4)', code_block=code_block)

    out_table.calculate_field('weekend_morning', 'weekend_is_in_range(!start_time!, !end_time!,  4, 8)', code_block=code_block)
    out_table.calculate_field('weekend_noon',    'weekend_is_in_range(!start_time!, !end_time!, 10, 12)', code_block=code_block)
    out_table.calculate_field('weekend_evening', 'weekend_is_in_range(!start_time!, !end_time!, 13, 17)', code_block=code_block)
    out_table.calculate_field('weekend_night',   'weekend_is_in_range(!start_time!, !end_time!, 19, 4)', code_block=code_block)

def axis(range):
    for axis in range:
        yield '{}_1'.format(axis)
        yield '{}_2'.format(axis)

def merge_feature_classes(feature_classes, target, delete=True):
    try:
        target.id
    except AttributeError:
        target = FeatureClass(target)

    target.delete_if_exists()

    iterator = iter(feature_classes)
    if delete:
        iterator.next().rename(target)
    else:
        iterator.next().copy(target)
    for subset in iterator:
        target.append(subset)
        if delete:
            subset.delete()

def create_tracks(in_fc, out_fc):
    def workday_is_in_range(start, end, min_hour, max_hour):
        if min_hour <= max_hour:
            start = (start.weekday() < 5 and min_hour <= start.hour < max_hour)
            end = (end.weekday() < 5 and min_hour <= end.hour < max_hour)
        else:
            start = (start.weekday() < 5 and (min_hour <= start.hour or start.hour < max_hour))
            end = (end.weekday() < 5 and (min_hour <= end.hour or end.hour < max_hour))
        return start or end

    def weekend_is_in_range(start, end, min_hour, max_hour):
        if min_hour <= max_hour:
            start = (start.weekday() >= 5 and min_hour <= start.hour < max_hour)
            end = (end.weekday() >= 5 and min_hour <= end.hour < max_hour)
        else:
            start = (start.weekday() >= 5 and (min_hour <= start.hour or start.hour < max_hour))
            end = (end.weekday() >= 5 and (min_hour <= end.hour or end.hour < max_hour))
        return start or end

    def _as_polyline(coordinates):
        points = (Point(*c) for c in coordinates)
        return Polyline(Array(points))

    def _create_polylines(rows):
        caxis = None
        ctrack = None
        coordinates = None
        start_time = None
        stop_time = None
        complete = None
        def create_row():
            duration = long((stop_time - start_time).total_seconds() * 10**3)

            weekend_morning = weekend_is_in_range(start_time, stop_time,  4, 8)
            weekend_noon = weekend_is_in_range(start_time, stop_time, 10, 12)
            weekend_evening = weekend_is_in_range(start_time, stop_time, 13, 17)
            weekend_night = weekend_is_in_range(start_time, stop_time, 19, 4)

            workday_morning = workday_is_in_range(start_time, stop_time,  4, 8)
            workday_noon = workday_is_in_range(start_time, stop_time, 10, 12)
            workday_evening = workday_is_in_range(start_time, stop_time, 13, 17)
            workday_night = workday_is_in_range(start_time, stop_time, 19, 4)

            return (_as_polyline(coordinates), caxis, ctrack, start_time, stop_time, duration, complete,
                weekend_morning, weekend_noon, weekend_evening, weekend_night,
                workday_morning, workday_noon, workday_evening, workday_night)

        for coords, axis, track, time, complete_axis_match in rows:


            if caxis is None:
                caxis = axis
            elif caxis != axis:
                if coordinates is not None:
                    yield create_row()
                    coordinates = None
                caxis = axis

            if ctrack is None:
                ctrack = track
            elif ctrack != track:
                if coordinates is not None:
                    yield create_row()
                    coordinates = None
                ctrack = track

            if coordinates is None:
                coordinates = [coords]
                complete = complete_axis_match
                start_time = stop_time = time
            else:
                coordinates.append(coords)
                stop_time = time

        if coordinates is not None:
            yield create_row()

    out_fc.create(geometry_type='POLYLINE', spatial_reference = SpatialReference(4326))

    out_fc.add_field('axis', 'TEXT')
    out_fc.add_field('track', 'TEXT')
    out_fc.add_field('start_time', 'DATE')
    out_fc.add_field('stop_time', 'DATE')
    out_fc.add_field('duration', 'LONG')
    out_fc.add_field('complete', 'SHORT')

    out_fc.add_field('weekend_morning', 'SHORT')
    out_fc.add_field('weekend_noon', 'SHORT')
    out_fc.add_field('weekend_evening', 'SHORT')
    out_fc.add_field('weekend_night', 'SHORT')

    out_fc.add_field('workday_morning', 'SHORT')
    out_fc.add_field('workday_noon', 'SHORT')
    out_fc.add_field('workday_evening', 'SHORT')
    out_fc.add_field('workday_night', 'SHORT')

    output_fields = [
        'SHAPE@', 'axis', 'track', 'start_time', 'stop_time', 'duration', 'complete',
        'weekend_morning', 'weekend_noon', 'weekend_evening', 'weekend_night',
        'workday_morning', 'workday_noon', 'workday_evening', 'workday_night'
    ]
    input_fields = ['SHAPE@XY', 'axis', 'track', 'time', 'complete_axis_match']
    sql_clause = (None, 'ORDER BY axis, track, time')

    with out_fc.insert(output_fields) as insert:
        with in_fc.search(input_fields, sql_clause=sql_clause) as rows:
            for polyline in _create_polylines(rows):
                insert.insertRow(polyline)

def calculate_statistics(model, fgdb):
    tracks_view = fgdb.feature_class('tracks').view()
    stops_view = fgdb.table('stops').view()
    measurement_view = fgdb.feature_class('measurements').view()

    def create_co2_consumption(postfix):
        # consumption_by_axis_
        out_table = fgdb.table('consumption_by_axis_' + postfix)
        measurement_view.statistics(
            out_table=out_table,
            statistics_fields=[('consumption','MEAN')],
            case_field=['axis'])
        out_table.rename_field('FREQUENCY', 'num_observations')
        out_table.rename_field('MEAN_consumption', 'consumption')
        # co2_by_axis_
        out_table = fgdb.table('co2_by_axis_' + postfix)
        measurement_view.statistics(
            out_table=out_table,
            statistics_fields=[('co2','MEAN')],
            case_field=['axis'])
        out_table.rename_field('FREQUENCY', 'num_observations')
        out_table.rename_field('MEAN_co2', 'co2')
        # consumption_by_axis_segment_
        out_table = fgdb.table('consumption_by_axis_segment_' + postfix)
        measurement_view.statistics(
            out_table=out_table,
            statistics_fields=[('consumption','MEAN')],
            case_field=['axis', 'segment'])
        out_table.rename_field('FREQUENCY', 'num_observations')
        out_table.rename_field('MEAN_consumption', 'consumption')
        out_table.add_join_field(['axis', 'segment'])
        # co2_by_axis_segment_
        out_table = fgdb.table('co2_by_axis_segment_' + postfix)
        measurement_view.statistics(
            out_table=out_table,
            statistics_fields=[('co2','MEAN')],
            case_field=['axis', 'segment'])
        out_table.rename_field('FREQUENCY', 'num_observations')
        out_table.rename_field('MEAN_co2', 'co2')
        out_table.add_join_field(['axis', 'segment'])

    def create_stops(postfix):
        # stops_by_axis_
        out_table = fgdb.table('stops_by_axis_' + postfix)
        stops_view.statistics(
            out_table=out_table,
            statistics_fields=[('duration', 'MEAN')],
            case_field=['axis'])
        out_table.rename_field('FREQUENCY', 'stops')
        out_table.rename_field('MEAN_duration', 'duration')
        # stops_by_axis_segment_
        out_table = fgdb.table('stops_by_axis_segment_' + postfix)
        stops_view.statistics(
            out_table=out_table,
            statistics_fields=[('duration', 'MEAN')],
            case_field=['axis','segment'])
        out_table.rename_field('FREQUENCY', 'stops')
        out_table.rename_field('MEAN_duration', 'duration')
        out_table.add_join_field(['axis', 'segment'])

    def create_travel_time_axis(postfix):
        # travel_time_by_axis_
        out_table = fgdb.table('travel_time_by_axis_' + postfix)
        # accomodate for single measurement tracks
        tracks_view.subset_selection('duration > 0')
        tracks_view.statistics(
            out_table=out_table,
            statistics_fields=[('duration', 'MEAN')],
            case_field=['axis'])
        out_table.rename_field('FREQUENCY', 'num_tracks')
        out_table.rename_field('MEAN_duration', 'travel_time')

    def create_travel_time_segment(postfix):
        # travel_time_by_axis_segment_
        tmp_table = fgdb.table('travel_time_by_axis_segment_' + postfix + '_tmp')

        measurement_view.statistics(
            out_table=tmp_table,
            statistics_fields=[('time', 'MIN'), ('time', 'MAX')],
            case_field=['axis', 'segment', 'track'])
        code_block = textwrap.dedent("""\
        from datetime import datetime
        def parse(s):
            if len(s) > 19:
                format_string = '%d.%m.%Y %H:%M:%S.%f'
            elif len(s) > 10:
                format_string = '%d.%m.%Y %H:%M:%S'
            else:
                format_string = '%d.%m.%Y'
            return datetime.strptime(s, format_string)
        def get_duration(start, end):
            return (parse(end)-parse(start)).total_seconds()*1000
        """)

        tmp_table.add_field('duration', 'LONG')
        tmp_table.calculate_field('duration', 'get_duration(!MIN_time!, !MAX_time!)', code_block=code_block)

        def get_length_of_track_on_segment(track, min_time, max_time):
            min_point = get_point(track, min_time)
            max_point = get_point(track, max_time)
            array = Array([min_point.firstPoint, max_point.firstPoint])
            polyline = Polyline(array, min_point.spatialReference)
            return polyline.getLength('GEODESIC', 'METERS')

        def get_segment_length(axis, segment):
            where_clause = SQL.and_((SQL.eq_('Achsen_ID', SQL.quote_(axis)), SQL.eq_('segment_id', segment)))
            with model.segments.search(['length'], where_clause=where_clause) as rows:
                for row in rows: return row[0]
            return None

        def get_point(track, axis, time):

            min_time = time.replace(microsecond=0) - timedelta(seconds=1)
            max_time = time.replace(microsecond=0) + timedelta(seconds=1)

            where_clause = SQL.and_((
                SQL.eq_('axis', SQL.quote(axis)),
                SQL.eq_('track', SQL.quote_(track)),
                SQL.is_between_('time', (
                    'date {}'.format(SQL.quote_(datetime.strftime(min_time, '%Y-%m-%d %H:%M:%S'))),
                    'date {}'.format(SQL.quote_(datetime.strftime(max_time, '%Y-%m-%d %H:%M:%S')))))))

            with measurement_view.search(['SHAPE@', 'time'], where_clause=where_clause) as rows:
                for row in rows:
                    if row[1] == time:
                        return row[0]
            return None


        with tmp_table.update(['axis', 'segment', 'track', 'MIN_time', 'MAX_time', 'duration']) as rows:
            for row in rows:
                axis, segment, track, min_time, max_time, duration = row
                tlength = get_length_of_track_on_segment(track, axis, min_time, max_time)
                slength = get_segment_length(axis, segment)
                if tlength > 0:
                    factor = slength/tlength
                    log.debug('travel_time stretching factor: %f', factor)
                    row[5] = row[5] * factor
                    rows.updateRow(row)
                else:
                    log.debug('can not scale travel time, length is zero')

        tmp_table_view = tmp_table.view()
        # accomodate for single measurement tracks
        tmp_table_view.new_selection('duration > 0')

        out_table = fgdb.table('travel_time_by_axis_segment_' + postfix)
        tmp_table_view.statistics(
            out_table=out_table,
            statistics_fields=[('duration', 'MEAN')],
            case_field=['axis','segment'])
        out_table.rename_field('FREQUENCY', 'num_tracks')
        out_table.rename_field('MEAN_duration', 'travel_time')
        out_table.add_join_field(['axis', 'segment'])

        tmp_table_view.delete()
        tmp_table.delete()

    try:
        measurement_view.clear_selection()
        create_co2_consumption('all')
        create_travel_time_segment('all')

        for time_of_week in ['workday', 'weekend']:
          for time_of_day in ['morning', 'evening', 'noon', 'night']:
            selector = '{}_{}'.format(time_of_week, time_of_day)
            measurement_view.new_selection(SQL.eq_(selector, 1))
            create_co2_consumption(selector)
            create_travel_time_segment(selector)

        stops_view.clear_selection()
        create_stops('all')

        for time_of_week in ['workday', 'weekend']:
          for time_of_day in ['morning', 'evening', 'noon', 'night']:
            selector = '{}_{}'.format(time_of_week, time_of_day)
            stops_view.new_selection(SQL.eq_(selector, 1))
            create_stops(selector)

        tracks_view.new_selection(SQL.eq_('complete', 1))
        create_travel_time_axis('all')

        for time_of_week in ['workday', 'weekend']:
          for time_of_day in ['morning', 'evening', 'noon', 'night']:
            selector = '{}_{}'.format(time_of_week, time_of_day)
            tracks_view.new_selection(SQL.and_((SQL.eq_('complete', 1), SQL.eq_(selector, 1))))
            create_travel_time_axis(selector)

    finally:
        tracks_view.delete()
        measurement_view.delete()
        stops_view.delete()

def find_passages_by_axis_segment(fgdb, stops_by_axis_segment_track, axis_segment, axis_track_segment, out_table):
    passages_without_stops = fgdb.table('passages_without_stops')
    passages_with_stops = fgdb.table('passages_with_stops')

    try:
        stops_by_axis_segment_track = stops_by_axis_segment_track.view()
        passages_by_axis_segment = axis_track_segment.view()
        try:
            passages_by_axis_segment.add_join('join_field', stops_by_axis_segment_track, 'join_field')

            # select all passages that have no stop
            passages_by_axis_segment.new_selection(SQL.or_((SQL.is_null_('%s.num_stops' % stops_by_axis_segment_track.id),
                                                            SQL.eq_('%s.num_stops' % stops_by_axis_segment_track.id, 0))))

            log.debug('passages_without_stops count: %d', passages_by_axis_segment.count())
            passages_by_axis_segment.statistics(
                out_table=passages_without_stops,
                statistics_fields=[('%s.track' % passages_by_axis_segment.id, 'COUNT')],
                case_field=(
                    '%s.axis' % passages_by_axis_segment.id,
                    '%s.segment' % passages_by_axis_segment.id))
            passages_without_stops.rename_field('%s_axis' % passages_by_axis_segment.id, 'axis')
            passages_without_stops.rename_field('%s_segment' % passages_by_axis_segment.id, 'segment')
            passages_without_stops.rename_field('FREQUENCY', 'passages_without_stops')
            passages_without_stops.delete_field('COUNT_%s_track' % passages_by_axis_segment.id)

            # select all passages that have stops
            passages_by_axis_segment.new_selection('%s.num_stops >= 0' % stops_by_axis_segment_track.id)
            log.debug('passages_with_stops count: %d', passages_by_axis_segment.count())
            passages_by_axis_segment.statistics(
                out_table=passages_with_stops,
                statistics_fields=[('%s.track' % passages_by_axis_segment.id, 'COUNT')],
                case_field=(
                    '%s.axis' % passages_by_axis_segment.id,
                    '%s.segment' % passages_by_axis_segment.id))
            passages_with_stops.rename_field('%s_axis' % passages_by_axis_segment.id, 'axis')
            passages_with_stops.rename_field('%s_segment' % passages_by_axis_segment.id, 'segment')
            passages_with_stops.rename_field('FREQUENCY', 'passages_with_stops')
            passages_with_stops.delete_field('COUNT_%s_track' % passages_by_axis_segment.id)

        finally:
            passages_by_axis_segment.delete()
            stops_by_axis_segment_track.delete()

        segments = axis_segment.view()
        try:
            segments.add_join('segment', passages_with_stops, 'segment')
            segments.add_join('segment', passages_without_stops, 'segment')

            fms = FieldMappings()
            fms.addTable('axis_segment')

            fields = ('passages_with_stops_OBJECTID',
                      'passages_with_stops_axis',
                      'passages_with_stops_segment',
                      'passages_without_stops_OBJECTID',
                      'passages_without_stops_axis',
                      'passages_without_stops_segment')

            for field in fields:
                fms.removeFieldMap(fms.findFieldMapIndex(field))


            out_table = out_table
            segments.to_table(out_table, field_mapping=fms)
            out_table.rename_field('passages_with_stops_passages_with_stops', 'passages_with_stops')
            out_table.rename_field('passages_without_stops_passages_without_stops', 'passages_without_stops')
            out_table.add_field('passages_overall', 'LONG')
            out_table.set_field_if_null('passages_with_stops', 0)
            out_table.set_field_if_null('passages_without_stops', 0)
            out_table.calculate_field('passages_overall', '!passages_with_stops! + !passages_without_stops!')
        finally:
            segments.delete()
    finally:
        passages_with_stops.delete_if_exists()
        passages_without_stops.delete_if_exists()

def find_passages_by_axis(fgdb, stops_by_axis_track, axis, axis_track, out_table):
    passages_without_stops = fgdb.table('passages_without_stops')
    passages_with_stops = fgdb.table('passages_with_stops')

    try:
        stops_by_axis_track = stops_by_axis_track.view()
        passages_by_axis = axis_track.view()

        try:
            passages_by_axis.add_join('join_field', stops_by_axis_track, 'join_field')


            # select all passages that have no stop
            passages_by_axis.new_selection(SQL.or_((SQL.is_null_('%s.num_stops' % stops_by_axis_track.id),
                                                    SQL.eq_('%s.num_stops' % stops_by_axis_track.id, 0))))

            log.debug('passages_without_stops count: %d', passages_by_axis.count())
            passages_by_axis.statistics(
                out_table=passages_without_stops,
                statistics_fields=[('%s.track' % passages_by_axis.id, 'COUNT')],
                case_field=('%s.axis' % passages_by_axis.id))
            passages_without_stops.rename_field('%s_axis' % passages_by_axis.id, 'axis')
            passages_without_stops.rename_field('FREQUENCY', 'passages_without_stops')
            passages_without_stops.delete_field('COUNT_%s_track' % passages_by_axis.id)

            # select all passages that have stops
            passages_by_axis.new_selection('%s.num_stops > 0' % stops_by_axis_track.id)
            log.debug('passages_with_stops count: %d', passages_by_axis.count())
            passages_by_axis.statistics(
                out_table=passages_with_stops,
                statistics_fields=[('%s.track' % passages_by_axis.id, 'COUNT')],
                case_field=('%s.axis' % passages_by_axis.id))
            passages_with_stops.rename_field('%s_axis' % passages_by_axis.id, 'axis')
            passages_with_stops.rename_field('FREQUENCY', 'passages_with_stops')
            passages_with_stops.delete_field('COUNT_%s_track' % passages_by_axis.id)

        finally:
            passages_by_axis.delete()
            stops_by_axis_track.delete()

        axes = axis.view()
        try:
            axes.add_join('axis', passages_with_stops, 'axis')
            axes.add_join('axis', passages_without_stops, 'axis')

            fms = FieldMappings()
            fms.addTable('axis')

            fields = ('passages_with_stops_OBJECTID',
                      'passages_with_stops_axis',
                      'passages_without_stops_OBJECTID',
                      'passages_without_stops_axis')

            for field in fields:
                fms.removeFieldMap(fms.findFieldMapIndex(field))


            out_table = out_table
            axes.to_table(out_table, field_mapping=fms)
            out_table.rename_field('passages_with_stops_passages_with_stops', 'passages_with_stops')
            out_table.rename_field('passages_without_stops_passages_without_stops', 'passages_without_stops')
            out_table.add_field('passages_overall', 'LONG')
            out_table.set_field_if_null('passages_with_stops', 0)
            out_table.set_field_if_null('passages_without_stops', 0)
            out_table.calculate_field('passages_overall', '!passages_with_stops! + !passages_without_stops!')
        finally:
            axes.delete()
    finally:
        passages_with_stops.delete_if_exists()
        passages_without_stops.delete_if_exists()

def create_axis_segment_table(fgdb, segments):
    """Creates a table all axis/segment combinatinons."""
    axis_segment = fgdb.table('axis_segment')
    segments.statistics(
        out_table=axis_segment,
        statistics_fields=[('segment_id', 'COUNT')],
        case_field=('Achsen_ID', 'segment_id'))
    axis_segment.rename_field('Achsen_ID', 'axis')
    axis_segment.rename_field('segment_id', 'segment')
    axis_segment.delete_field('FREQUENCY')
    axis_segment.delete_field('COUNT_segment_id')
    return axis_segment

def create_axis_table(fgdb, segments):
    """Creates a table all axis/segment combinatinons."""
    out_table = fgdb.table('axis')
    segments.statistics(
        out_table=out_table,
        statistics_fields=[('Achsen_ID', 'COUNT')],
        case_field=('Achsen_ID'))
    out_table.rename_field('Achsen_ID', 'axis')
    out_table.delete_field('FREQUENCY')
    out_table.delete_field('COUNT_Achsen_ID')
    return out_table

def create_segments_per_track_table(fgdb, axis_track_segment, num_segments_per_axis):
    """Creates a table containing the number of segments per track."""
    num_segments_per_track = fgdb.table('num_segments_per_track')
    axis_track_segment.statistics(
        out_table=num_segments_per_track,
        statistics_fields=[('segment', 'COUNT'), ('start_time', 'MIN'), ('end_time', 'MAX')],
        case_field=('axis', 'track'))

    num_segments_per_track.rename_field('FREQUENCY', 'segments')
    num_segments_per_track.rename_field('MIN_start_time', 'start_time')
    num_segments_per_track.rename_field('MAX_end_time', 'end_time')
    num_segments_per_track.delete_field('COUNT_segment')
    num_segments_per_track.add_join_field(['axis','track'])

    num_segments_per_track.add_field('complete', 'SHORT')
    num_segments_per_track.add_field('weekend_morning', 'SHORT')
    num_segments_per_track.add_field('weekend_evening', 'SHORT')
    num_segments_per_track.add_field('weekend_night', 'SHORT')
    num_segments_per_track.add_field('weekend_noon', 'SHORT')

    num_segments_per_track.add_field('workday_morning', 'SHORT')
    num_segments_per_track.add_field('workday_evening', 'SHORT')
    num_segments_per_track.add_field('workday_night', 'SHORT')
    num_segments_per_track.add_field('workday_noon', 'SHORT')

    view = num_segments_per_track.view()

    try:
        view.add_join('axis', num_segments_per_axis, 'axis')

        code_block = textwrap.dedent("""\
        from datetime import datetime

        def parse(s):
            format_string = None
            if len(s) > 19:
                format_string = '%d.%m.%Y %H:%M:%S.%f'
            elif len(s) > 10:
                format_string = '%d.%m.%Y %H:%M:%S'
            else:
                format_string = '%d.%m.%Y'
            return datetime.strptime(s, format_string)


        def is_complete(axis_segments, track_segments):
            return True if axis_segments == track_segments else False

        def workday_is_in_range(start, end, min_hour, max_hour):
            start = parse(start)
            end = parse(end)
            if min_hour <= max_hour:
                start = (start.weekday() < 5 and min_hour <= start.hour < max_hour)
                end = (end.weekday() < 5 and min_hour <= end.hour < max_hour)
            else:
                start = (start.weekday() < 5 and (min_hour <= start.hour or start.hour < max_hour))
                end = (end.weekday() < 5 and (min_hour <= end.hour or end.hour < max_hour))
            return start or end

        def weekend_is_in_range(start, end, min_hour, max_hour):
            start = parse(start)
            end = parse(end)
            if min_hour <= max_hour:
                start = (start.weekday() >= 5 and min_hour <= start.hour < max_hour)
                end = (end.weekday() >= 5 and min_hour <= end.hour < max_hour)
            else:
                start = (start.weekday() >= 5 and (min_hour <= start.hour or start.hour < max_hour))
                end = (end.weekday() >= 5 and (min_hour <= end.hour or end.hour < max_hour))
            return start or end
        """)

        view.calculate_field('workday_morning',  'workday_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!,  4, 8)', code_block=code_block)
        view.calculate_field('workday_evening',  'workday_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 13, 17)', code_block=code_block)
        view.calculate_field('workday_noon',     'workday_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 10, 12)', code_block=code_block)
        view.calculate_field('workday_night',    'workday_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 19, 4)', code_block=code_block)

        view.calculate_field('weekend_morning',  'weekend_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!,  4, 8)', code_block=code_block)
        view.calculate_field('weekend_evening',  'weekend_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 13, 17)', code_block=code_block)
        view.calculate_field('weekend_noon',     'weekend_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 10, 12)', code_block=code_block)
        view.calculate_field('weekend_night',    'weekend_is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 19, 4)', code_block=code_block)

        view.calculate_field('complete', 'is_complete(!num_segments_per_axis.segments!, !num_segments_per_track.segments!)', code_block=code_block)
    finally:
        view.delete()


    t = num_segments_per_track.statistics(
        out_table=fgdb.table('num_tracks_per_axis'),
        statistics_fields=[
            ('complete', 'SUM'),
            ('weekend_morning', 'SUM'),
            ('weekend_evening', 'SUM'),
            ('weekend_noon', 'SUM')
            ('weekend_night', 'SUM'),
            ('workday_morning', 'SUM'),
            ('workday_evening', 'SUM'),
            ('workday_noon', 'SUM')
            ('workday_night', 'SUM')],
        case_field='axis')
    t.rename_field('SUM_complete', 'complete')
    t.rename_field('SUM_workday_morning', 'workday_morning')
    t.rename_field('SUM_workday_evening', 'workday_evening')
    t.rename_field('SUM_workday_noon', 'workday_noon')
    t.rename_field('SUM_workday_night', 'workday_night')

    t.rename_field('SUM_weekend_morning', 'weekend_morning')
    t.rename_field('SUM_weekend_evening', 'weekend_evening')
    t.rename_field('SUM_weekend_noon', 'weekend_noon')
    t.rename_field('SUM_weekend_night', 'weekend_night')
    t.rename_field('FREQUENCY', 'sum')

    return num_segments_per_track

def create_segments_per_axis_table(fgdb, axis_segment_fc):
    """Creates a table containing the number of segments per axis."""
    num_segments_per_axis = fgdb.table('num_segments_per_axis')
    axis_segment_fc.statistics(
        out_table=num_segments_per_axis,
        statistics_fields=[('segment_id', 'COUNT')],
        case_field=('Achsen_ID'))
    num_segments_per_axis.rename_field('Achsen_ID', 'axis')
    num_segments_per_axis.rename_field('FREQUENCY', 'segments')
    num_segments_per_axis.delete_field('COUNT_segment_id')
    return num_segments_per_axis

def find_passages(fgdb, axis_model):
    measurements = fgdb.feature_class('measurements').view()
    stops = fgdb.table('stops').view()
    axis_segment = create_axis_segment_table(fgdb, axis_model.segments)
    axis = create_axis_table(fgdb, axis_model.segments)
    try:
        def create_passages_by_axis_segment_table(postfix, sql=None):
            def create_axis_track_segment_table(name, sql=None):
                if sql is None: measurements.clear_selection()
                else: measurements.new_selection(sql)
                out_table = fgdb.table(name)
                measurements.statistics(
                    out_table=out_table,
                    statistics_fields=[('objectid', 'COUNT'), ('time', 'MIN'), ('time', 'MAX')],
                    case_field=('axis','track','segment'))
                out_table.delete_field('COUNT_objectid')
                out_table.delete_field('FREQUENCY')
                out_table.rename_field('MIN_time', 'start_time')
                out_table.rename_field('MAX_time', 'end_time')
                out_table.add_join_field(['axis', 'segment', 'track'])
                return out_table

            def create_stops_by_axis_segment_track(name, sql=None):
                if sql is None: stops.clear_selection()
                else: stops.new_selection(sql)
                out_table = fgdb.table(name)
                stops.statistics(
                    out_table=out_table,
                    statistics_fields=[('duration', 'MEAN')],
                    case_field=['axis','segment','track'])
                out_table.rename_field('FREQUENCY', 'num_stops')
                out_table.rename_field('MEAN_duration', 'duration')
                out_table.add_join_field(['axis', 'segment', 'track'])
                return out_table

            axis_track_segment = create_axis_track_segment_table('axis_track_segment_' + postfix, sql)
            stops_by_axis_segment_track = create_stops_by_axis_segment_track('stops_by_axis_segment_track_' + postfix, sql)
            try:
                find_passages_by_axis_segment(
                    fgdb,
                    stops_by_axis_segment_track,
                    axis_segment,
                    axis_track_segment,
                    fgdb.table('passages_by_axis_segment_' + postfix))
            finally:
                stops_by_axis_segment_track.delete_if_exists()
                axis_track_segment.delete_if_exists()

        def create_passages_by_axis_table(postfix, sql):
            def create_axis_track_table(name, sql=None):
                if sql is None:
                    measurements.new_selection(SQL.eq_('complete_axis_match', 1))
                else:
                    measurements.new_selection(SQL.and_((SQL.eq_('complete_axis_match', 1), sql)))
                out_table = fgdb.table(name)
                measurements.statistics(
                    out_table=out_table,
                    statistics_fields=[('objectid', 'COUNT'), ('time', 'MIN'), ('time', 'MAX')],
                    case_field=('axis', 'track'))
                out_table.delete_field('COUNT_objectid')
                out_table.delete_field('FREQUENCY')
                out_table.rename_field('MIN_time', 'start_time')
                out_table.rename_field('MAX_time', 'end_time')
                out_table.add_join_field(['axis', 'track'])
                return out_table

            def create_stops_by_axis_track(name, sql=None):
                if sql is None:
                    stops.new_selection(SQL.eq_('complete', 1))
                else:
                    stops.new_selection(SQL.and_((SQL.eq_('complete', 1), sql)))
                out_table = fgdb.table(name)
                stops.statistics(
                    out_table=out_table,
                    statistics_fields=[('duration', 'MEAN')],
                    case_field=['axis', 'track'])
                out_table.rename_field('FREQUENCY', 'num_stops')
                out_table.rename_field('MEAN_duration', 'duration')
                out_table.add_join_field(['axis', 'track'])
                return out_table

            axis_track = create_axis_track_table('axis_track_' + postfix, sql)
            stops_by_axis_track = create_stops_by_axis_track('stops_by_axis_track_' + postfix, sql)
            try:
                find_passages_by_axis(
                    fgdb,
                    stops_by_axis_track,
                    axis,
                    axis_track,
                    fgdb.table('passages_by_axis_' + postfix))
            finally:
                stops_by_axis_track.delete_if_exists()
                axis_track.delete_if_exists()


        create_passages_by_axis_segment_table('all', None)
        create_passages_by_axis_segment_table('workday_morning', SQL.eq_('workday_morning', 1))
        create_passages_by_axis_segment_table('workday_evening', SQL.eq_('workday_evening', 1))
        create_passages_by_axis_segment_table('workday_noon', SQL.eq_('workday_noon', 1))
        create_passages_by_axis_segment_table('workday_night', SQL.eq_('workday_night', 1))
        create_passages_by_axis_segment_table('weekend_morning', SQL.eq_('weekend_morning', 1))
        create_passages_by_axis_segment_table('weekend_evening', SQL.eq_('weekend_evening', 1))
        create_passages_by_axis_segment_table('weekend_noon', SQL.eq_('weekend_noon', 1))
        create_passages_by_axis_segment_table('weekend_night', SQL.eq_('weekend_night', 1))
        create_passages_by_axis_table('all', None)
        create_passages_by_axis_table('workday_morning', SQL.eq_('workday_morning', 1))
        create_passages_by_axis_table('workday_evening', SQL.eq_('workday_evening', 1))
        create_passages_by_axis_table('workday_noon', SQL.eq_('workday_noon', 1))
        create_passages_by_axis_table('workday_night', SQL.eq_('workday_night', 1))
        create_passages_by_axis_table('weekend_morning', SQL.eq_('weekend_morning', 1))
        create_passages_by_axis_table('weekend_evening', SQL.eq_('weekend_evening', 1))
        create_passages_by_axis_table('weekend_noon', SQL.eq_('weekend_noon', 1))
        create_passages_by_axis_table('weekend_night', SQL.eq_('weekend_night', 1))

    finally:
        stops.delete()
        axis_segment.delete_if_exists()
        axis.delete_if_exists()

def add_time_segment_fields(feature_class):
    code_block = textwrap.dedent("""\
    from datetime import datetime

    def parse(s):
        format_string = None
        if len(s) > 19:
            format_string = '%d.%m.%Y %H:%M:%S.%f'
        elif len(s) > 10:
            format_string = '%d.%m.%Y %H:%M:%S'
        else:
            format_string = '%d.%m.%Y'
        return datetime.strptime(s, format_string)

    def workday_is_in_range(time, min_hour, max_hour):
        time = parse(time)
        if min_hour <= max_hour:
            return (time.weekday() < 5 and min_hour <= time.hour < max_hour)
        else:
            return (time.weekday() < 5 and (min_hour <= time.hour or time.hour < max_hour))

    def weekend_is_in_range(time, min_hour, max_hour):
        time = parse(time)
        if min_hour <= max_hour:
            return (time.weekday() >= 5 and min_hour <= time.hour < max_hour)
        else:
            return (time.weekday() >= 5 and (min_hour <= time.hour or time.hour < max_hour))
    """)


    for time_of_week in ['workday', 'weekend']:
        for time_of_day in ['morning', 'evening', 'noon', 'night']:
            selector = '{}_{}'.format(time_of_week, time_of_day)
            feature_class.add_field(selector, 'SHORT')

    feature_class.calculate_field('workday_morning', 'workday_is_in_range(!time!,  4, 8)', code_block=code_block)
    feature_class.calculate_field('workday_noon',    'workday_is_in_range(!time!, 10, 12)', code_block=code_block)
    feature_class.calculate_field('workday_evening', 'workday_is_in_range(!time!, 13, 17)', code_block=code_block)
    feature_class.calculate_field('workday_night',   'workday_is_in_range(!time!, 19, 4)', code_block=code_block)

    feature_class.calculate_field('weekend_morning', 'weekend_is_in_range(!time!,  4, 8)', code_block=code_block)
    feature_class.calculate_field('weekend_noon',    'weekend_is_in_range(!time!, 10, 12)', code_block=code_block)
    feature_class.calculate_field('weekend_evening', 'weekend_is_in_range(!time!, 13, 17)', code_block=code_block)
    feature_class.calculate_field('weekend_night',   'weekend_is_in_range(!time!, 19, 4)', code_block=code_block)

    feature_class.add_index(['segment'], 'segment_idx')
    feature_class.add_index(['axis'], 'axis_idx')
    feature_class.add_index(['track'], 'track_idx')
    feature_class.add_index(['time'], 'time_idx')
    feature_class.add_index(['complete_axis_match'], 'complete_axis_match_idx')

    for time_of_week in ['workday', 'weekend']:
        for time_of_day in ['morning', 'evening', 'noon', 'night']:
            selector = '{}_{}'.format(time_of_week, time_of_day)
            feature_class.add_index([selector], '{}_idx'.format(selector))

