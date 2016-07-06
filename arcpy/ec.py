import os
import csv
import textwrap
from arcpy import SpatialReference, Array, Point, Polyline, FieldMappings, env
from ooarcpy import FeatureClass, FileGDB
from itertools import izip, islice, tee, groupby
from datetime import timedelta, datetime
from utils import first, nwise, min_max, SQL, gzip_file
import logging

log = logging.getLogger(__name__)

class TrackMatchingResult(object):
    MAX_TIME_SPAN = timedelta(seconds=10)

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
            non_consecutive = not (b.min_idx <= a.max_idx + 1 <= b.max_idx)

            # same node, but a too big time difference
            too_big_time_difference = (a.idx == b.idx and (a.max_time - b.min_time) < TrackMatchingResult.MAX_TIME_SPAN)

            if non_consecutive or too_big_time_difference:
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
        return not self.min_idx > 0

    @property
    def includes_last_node(self):
        return not self.max_idx < self.axis_node_count


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
        finally:
            self.node_buffer_fl.delete()
            #self.axis_mbr_fl.delete()
            self.measurements_fl.delete()
            self.trajectories_fl.delete()
            self.axis_segment_fl.delete()
            self.tracks_fl.delete()
            #self.node_buffer_fc.delete_if_exists()
            #self.node_fc.delete_if_exists()
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

            with nfc.insert(fnames + ['complete_axis_match']) as insert:
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
    SAMPLING_RATE = timedelta(seconds=5)

    def __init__(self, axis, segment, track, start, stop):
        self.track = track
        self.segment = segment
        self.axis = axis
        # extend the time by the sampling rate to
        # fix single measurement stops
        self.start = start - Stop.SAMPLING_RATE/2
        self.stop = stop + Stop.SAMPLING_RATE/2

    def __str__(self):
        return 'Axis: {0}, Segment: {1}, Track: {2}, Duration: {3}'.format(self.axis, self.segment, self.track, self.duration)

    @property
    def duration(self):
        return self.stop - self.start

    @staticmethod
    def find(fc, stop_start_threshold=5, stop_end_threshold=10):

        fields = ['axis', 'segment', 'track', 'time', 'speed']
        sql_clause = (None, 'ORDER BY axis, track, time')

        ctrack = None
        csegment = None
        caxis = None
        stop_start = None
        stop_end = None
        is_stop = False

        def create_stop():
            return Stop(axis=caxis,
                        segment=csegment,
                        track=ctrack,
                        start=stop_start,
                        stop=stop_end)

        with fc.search(fields, sql_clause = sql_clause) as rows:
            for axis, segment, track, time, speed in rows:
                change = caxis != axis or ctrack != track or csegment != segment
                if is_stop and change:
                    yield create_stop()
                    is_stop = False
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
    field_names = ['axis', 'segment', 'track', 'start_time', 'end_time', 'duration']
    field_types = ['TEXT', 'LONG',    'TEXT',  'DATE',       'DATE',     'LONG'    ]

    out_table.delete_if_exists()
    out_table.create()


    for field_name, field_type in zip(field_names, field_types):
        out_table.add_field(field_name, field_type)

    with out_table.insert(field_names) as insert:
        for stop in Stop.find(in_fc, stop_start_threshold=5, stop_end_threshold=10):
            duration = long(stop.duration.total_seconds() * 1000)
            insert.insertRow((stop.axis, stop.segment, stop.track, stop.start, stop.stop, duration))

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
    def _as_polyline(coordinates):
        points = (Point(*c) for c in coordinates)
        return Polyline(Array(points))

    def _create_polylines(rows):
        caxis = None
        ctrack = None
        coordinates = None
        start_time = None
        stop_time = None

        def create_row(): return (_as_polyline(coordinates), caxis, ctrack, start_time, stop_time)

        for coords, axis, track, time in rows:

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



    output_fields = ['SHAPE@', 'axis', 'track', 'start_time', 'stop_time']
    input_fields = ['SHAPE@XY', 'axis', 'track', 'time']
    sql_clause = (None, 'ORDER BY axis, track, time')

    with out_fc.insert(output_fields) as insert:
        with in_fc.search(input_fields, sql_clause=sql_clause) as rows:
            for polyline in _create_polylines(rows):
                insert.insertRow(polyline)

def calculate_statistics(measurements_fc, stops_table, fgdb):
    def stats(case_fields, out_name):
        out_table = fgdb.table(out_name)
        measurements_fc.statistics(
            out_table=out_table,
            statistics_fields=[
                ('co2', 'MEAN'),
                ('consumption','MEAN')
            ],
            case_field=case_fields)
        out_table.rename_field('FREQUENCY', 'num_observations')
        out_table.rename_field('MEAN_co2', 'co2')
        out_table.rename_field('MEAN_consumption', 'consumption')
        out_table.add_join_field(case_fields)

    def stop_stats(case_fields, out_name):
        out_table = fgdb.table(out_name)
        stops_table.statistics(
            out_table=out_table,
            statistics_fields=[('duration', 'MEAN')],
            case_field=case_fields)
        out_table.rename_field('FREQUENCY', 'num_stops')
        out_table.rename_field('MEAN_duration', 'duration')
        out_table.add_join_field(case_fields)

    stats(['axis'], 'per_axis')
    stats(['axis', 'segment'], 'per_segment')
    stats(['axis', 'track'], 'per_axis_per_track')
    stats(['axis', 'track', 'segment'], 'per_segment_per_track')

    stop_stats(['axis'], 'stops_per_axis')
    stop_stats(['axis', 'segment'], 'stops_per_segment')
    stop_stats(['axis', 'track'], 'stops_per_axis_per_track')
    stop_stats(['axis', 'track', 'segment'], 'stops_per_segment_per_track')

def find_passages_per_segment(fgdb, axis_track_segment, axis_segment):
    passages_without_stops = fgdb.table('passages_without_stops')
    passages_with_stops = fgdb.table('passages_with_stops')
    try:
        passages_per_segment = axis_track_segment.view()
        try:
            passages_per_segment.add_join('join_field', fgdb.table('stops_per_segment_per_track'), 'join_field')

            # select all passages that have no stop
            passages_per_segment.new_selection(SQL.or_((SQL.is_null_('stops_per_segment_per_track.num_stops'),
                                                        SQL.eq_('stops_per_segment_per_track.num_stops', 0))))

            passages_per_segment.statistics(
                out_table=passages_without_stops,
                statistics_fields=[('axis_track_segment.track', 'COUNT')],
                case_field=('axis_track_segment.axis', 'axis_track_segment.segment'))
            passages_without_stops.rename_field('axis_track_segment_axis', 'axis')
            passages_without_stops.rename_field('axis_track_segment_segment', 'segment')
            passages_without_stops.rename_field('FREQUENCY', 'passages_without_stops')
            passages_without_stops.delete_field('COUNT_axis_track_segment_track')

            # select all passages that have stops
            passages_per_segment.new_selection('stops_per_segment_per_track.num_stops >= 0')

            passages_per_segment.statistics(
                out_table=passages_with_stops,
                statistics_fields=[('axis_track_segment.track', 'COUNT')],
                case_field=('axis_track_segment.axis', 'axis_track_segment.segment'))
            passages_with_stops.rename_field('axis_track_segment_axis', 'axis')
            passages_with_stops.rename_field('axis_track_segment_segment', 'segment')
            passages_with_stops.rename_field('FREQUENCY', 'passages_with_stops')
            passages_with_stops.delete_field('COUNT_axis_track_segment_track')

        finally:
            passages_per_segment.delete()

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


            passages_per_segment = fgdb.table('passages_per_segment')
            segments.to_table(passages_per_segment, field_mapping=fms)
            passages_per_segment.rename_field('passages_with_stops_passages_with_stops', 'passages_with_stops')
            passages_per_segment.rename_field('passages_without_stops_passages_without_stops', 'passages_without_stops')
            passages_per_segment.add_field('passages_overall', 'LONG')
            passages_per_segment.set_field_if_null('passages_with_stops', 0)
            passages_per_segment.set_field_if_null('passages_without_stops', 0)
            passages_per_segment.calculate_field('passages_overall', '!passages_with_stops! + !passages_without_stops!')

        finally:
            segments.delete()
    finally:
        passages_with_stops.delete_if_exists()
        passages_without_stops.delete_if_exists()

def create_axis_track_segment_table(fgdb, measurements_fc):
    """Creates a table containing all axis/track/segment combinations."""
    axis_track_segment = fgdb.table('axis_track_segment')
    measurements_fc.statistics(
        out_table=axis_track_segment,
        statistics_fields=[('id', 'COUNT'), ('time', 'MIN'), ('time', 'MAX')],
        case_field=('axis','track','segment'))
    axis_track_segment.delete_field('COUNT_id')
    axis_track_segment.delete_field('FREQUENCY')
    axis_track_segment.add_join_field(['axis', 'track', 'segment'])
    axis_track_segment.rename_field('MIN_time', 'start_time')
    axis_track_segment.rename_field('MAX_time', 'end_time')
    return axis_track_segment

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
    num_segments_per_track.add_field('morning', 'SHORT')
    num_segments_per_track.add_field('evening', 'SHORT')
    num_segments_per_track.add_field('noon', 'SHORT')

    view = num_segments_per_track.view()

    try:
        view.add_join('axis', num_segments_per_axis, 'axis')

        code_block = textwrap.dedent("""\
        from datetime import datetime

        def parse(s):
            format_string = '%d.%m.%Y %H:%M:%S.%f' if len(s)>19 else '%d.%m.%Y %H:%M:%S'
            return datetime.strptime(s, format_string)

        def is_complete(axis_segments, track_segments):
            return True if axis_segments == track_segments else False

        def is_in_range(start, end, min_hour, max_hour):
            start = parse(start)
            end = parse(end)
            start = (0 <= start.weekday() < 5 and min_hour <= start < max_hour)
            end = (0 <= end.weekday() < 5 and min_hour <= end < max_hour)
            return start or end
        """)

        view.calculate_field('morning', 'is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!,  6, 10)', code_block=code_block)
        view.calculate_field('evening', 'is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 15, 19)', code_block=code_block)
        view.calculate_field('noon',    'is_in_range(!num_segments_per_track.start_time!, !num_segments_per_track.end_time!, 12, 14)', code_block=code_block)
        view.calculate_field('complete', 'is_complete(!num_segments_per_axis.segments!, !num_segments_per_track.segments!)', code_block=code_block)
    finally:
        view.delete()


    t = num_segments_per_track.statistics(
        out_table=fgdb.table('num_tracks_per_axis'),
        statistics_fields=[
            ('complete', 'SUM'),
            ('morning', 'SUM'),
            ('evening', 'SUM'),
            ('noon', 'SUM')],
        case_field='axis')
    t.rename_field('SUM_complete', 'complete')
    t.rename_field('SUM_morning', 'morning')
    t.rename_field('SUM_evening', 'evening')
    t.rename_field('SUM_noon', 'noon')
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

def find_passages_per_axis(fgdb):
    pass

def find_passages(fgdb, axis_model, measurements_fc):
    axis_segment = create_axis_segment_table(fgdb, axis_model.segments)
    axis_track_segment = create_axis_track_segment_table(fgdb, measurements_fc)
    try:
        num_segments_per_axis = create_segments_per_axis_table(fgdb, axis_model.segments)
        num_segments_per_track = create_segments_per_track_table(fgdb, axis_track_segment, num_segments_per_axis)
        find_passages_per_axis(fgdb)
        find_passages_per_segment(fgdb, axis_track_segment, axis_segment)


    finally:
        pass
        axis_track_segment.delete()
        axis_segment.delete()
