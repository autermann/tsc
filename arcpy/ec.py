import arcpy
import textwrap
import os
import csv
from itertools import izip, islice, tee
from datetime import timedelta


class TrackMatchingResult:
    def __init__(self, track, matches, num_consecutive_results):
        self.track = track
        self.matches = [x for x in self._filter_matches(matches) if len(x) > num_consecutive_results]

    def __str__(self):
        return '<track: {}, matches: {}>'.format(self.track, [str(match) for match in self.matches])

    def __len__(self):
        return len(self.matches)

    def _filter_matches(self, matches):
        matches = sorted(matches, key=lambda x: x.time)

        current = None
        max_time_span = timedelta(seconds=20)

        for a, b in nwise(matches, 2):

            if a.max_idx > b.min_idx or (a.idx == b.idx and (a.max_time - b.min_time) < max_time_span):
                if current is not None:
                    yield current
                    current = None
                continue

            # if both are crossed:
            if a is not None and b is not None:
                if a.max_time <= b.min_time or a.time == b.time:
                    if current is None:
                        current = a.merge(b)
                    else:
                        current = current.merge(b)
            elif current is not None:
                yield current
                current = None

        if current is not None:
            yield current

    def as_sql_clause(self):
        """Converts this result to a SQL clause."""
        return SQL.and_((SQL.eq_('track', self.track), SQL.or_(x.as_sql_clause() for x in self.matches)))

class NodeMatchingResult(object):
    def __init__(self, min_time, max_time, min_idx, max_idx = None):
        if max_idx is None:
            max_idx = min_idx
        if min_idx > max_idx:
            raise ValueError('min_idx (%s) > max_idx (%s)' % (min_idx, max_idx))
        if min_time > max_time:
            raise ValueError('min_time (%s) > max_time (%s' % (min_time, max_time))

        self.idx = (min_idx, max_idx)
        self.time = (min_time, max_time)

    def __len__(self):
        return self.idx[1] - self.idx[0] + 1

    def __str__(self):
        if self.min_idx == self.max_idx:
            return '<Node: {0}, {1}--{2}>'.format(self.min_idx, str(self.min_time), str(self.max_time))
        else:
            return '<Node: {0}--{1}, {2}--{3}>'.format(self.min_idx, self.max_idx, str(self.min_time), str(self.max_time))

    def as_sql_clause(self):
        """
        Convert this result to a SQL-WHERE-clause that can be applied to the
        measurements table.
        """
        return SQL.is_between_('time', self.time)

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

    def merge(self, other):
        """Merges this result with another result."""
        return NodeMatchingResult(
            self.time[0], other.time[1],
            self.idx[0], other.idx[1])


def create_axis_subsets(measurements_fc,
                        trajectories_fc,
                        tracks_fc,
                        axis_model,
                        out_dir = None,
                        out_name = 'outputs.gdb',
                        axes = None,
                        time = None,
                        node_tolerance = 30):

    matcher = TrackMatcher(
        measurements_fc=measurements_fc,
        trajectories_fc=trajectories_fc,
        tracks_fc=tracks_fc,
        axes=axes,
        time=time,
        out_dir=out_dir,
        out_name=out_name,
        node_tolerance=node_tolerance,
        axis_model=axis_model)
    matcher.analyze()

class AxisModel(object):
    def __init__(self, axis_segment_fc, node_start_fc,
                 node_influence_fc, node_lsa_fc):
        self.axis_segment_fc = axis_segment_fc
        self.node_start_fc = node_start_fc
        self.node_influence_fc = node_influence_fc
        self.node_lsa_fc = node_lsa_fc

    @staticmethod
    def for_dir(directory):
        return AxisModel(
            axis_segment_fc = os.path.join(directory, 'Achsensegmente.shp'),
            node_influence_fc = os.path.join(directory, 'N_Einflussbereich.shp'),
            node_lsa_fc = os.path.join(directory, 'K_LSA.shp'),
            node_start_fc = os.path.join(directory, 'S_Start.shp'))

def get_all_axes(axis_model):
        def get_axes(fc, field='Achsen_ID'):
            sql_clause = ('DISTINCT', None)
            with arcpy.da.SearchCursor(fc, [field], sql_clause=sql_clause) as rows:
                for row in rows:
                    yield row[0]
        feature_classes = (axis_model.axis_segment_fc, axis_model.node_start_fc,
                           axis_model.node_lsa_fc, axis_model.node_influence_fc)
        axes = set(axis for feature_class in feature_classes for axis in get_axes(feature_class))
        return sorted(axes)

class TrackMatcher(object):
    TYPE_START = 1
    TYPE_LSA = 2
    TYPE_INFLUENCE = 3
    EPSG_4326 = arcpy.SpatialReference(4326)

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

        self.out_dir = out_dir if out_dir is not None else arcpy.env.workspace
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        self.out_name = out_name
        self.fgdb = os.path.join(self.out_dir, self.out_name)
        self.axis_model = axis_model

        self.measurements_fc = measurements_fc
        self.measurements_fl = 'measurements_fl'
        self.trajectories_fc = trajectories_fc
        self.trajectories_fl = 'trajectories_fl'
        self.tracks_fc = tracks_fc
        self.axis_segment_fc = axis_model.axis_segment_fc
        self.axis_segment_fl = 'axis_segment_fl'
        self.node_start_fc = axis_model.node_start_fc
        self.node_influence_fc = axis_model.node_influence_fc
        self.node_lsa_fc = axis_model.node_lsa_fc
        self.axis_mbr_fc = None
        self.axis_mbr_fl = 'axis_mbr_fl'
        self.node_fc = None
        self.node_buffer_fc = None
        self.node_buffer_fl = 'node_buffer_fl'
        self.measurements_by_axis_fc = dict()

        self.axis_ids = axes
        self.time = time
        self.node_tolerance = node_tolerance

    def analyze(self):
        if self.axis_ids is None:
            self.axis_ids = get_all_axes(self.axis_model)

        if not arcpy.Exists(self.fgdb):
            arcpy.management.CreateFileGDB(self.out_dir, self.out_name)

        arcpy.management.MakeFeatureLayer(self.measurements_fc, self.measurements_fl)
        arcpy.management.MakeFeatureLayer(self.trajectories_fc, self.trajectories_fl)
        arcpy.management.MakeFeatureLayer(self.axis_segment_fc, self.axis_segment_fl)

        # join the different node feature classes
        self.node_fc = self.create_node_feature_class()
        # create buffers around nodes
        self.node_buffer_fc = self.create_node_buffer_feature_class()
        # create the MBR for all axis
        self.axis_mbr_fc = self.create_axis_mbr_feature_class()

        arcpy.management.MakeFeatureLayer(self.node_buffer_fc, self.node_buffer_fl)
        arcpy.management.MakeFeatureLayer(self.axis_mbr_fc, self.axis_mbr_fl)

        try:
            target = os.path.join(self.fgdb, 'measurements')
            subsets = [self.create_ec_subset_for_axis(axis) for axis in self.axis_ids]
            merge_feature_classes(subsets, target)
        finally:
            arcpy.management.Delete(self.node_buffer_fl)
            arcpy.management.Delete(self.axis_mbr_fl)
            arcpy.management.Delete(self.node_buffer_fc)
            arcpy.management.Delete(self.node_fc)
            arcpy.management.Delete(self.axis_mbr_fc)
            arcpy.management.Delete(self.measurements_fl)
            arcpy.management.Delete(self.trajectories_fl)
            arcpy.management.Delete(self.axis_segment_fl)

    def create_node_buffer_feature_class(self):
        return arcpy.analysis.Buffer(self.node_fc,
                os.path.join(self.fgdb, 'nodes_buffer'),
                str(self.node_tolerance) + ' Meters')

    def create_ec_subset_for_axis(self, axis):
        matches = self.get_track_matches_for_axis(axis)

        fc = self.measurements_fc
        # create the feature class

        nfc = os.path.join(self.fgdb, 'ec_subset_for_axis_{}'.format(axis))

        if arcpy.Exists(nfc):
            arcpy.management.Delete(nfc)

        nfc = arcpy.management.CreateFeatureclass(
            self.fgdb,
            'ec_subset_for_axis_{}'.format(axis),
            geometry_type = 'POINT',
            spatial_reference = arcpy.SpatialReference(4326))

        self.measurements_by_axis_fc[axis] = nfc

        # get the fields to create (ignore the geometry and OID field)
        fields = [(field.name, field.type) for field in arcpy.ListFields(fc) if field.type != 'OID' and field.type != 'Geometry']
        # add the fields to the feature class and change the type of track to string
        for fname, ftype in fields:
            arcpy.management.AddField(nfc, fname, 'text' if fname == 'track' else ftype)

        # if we have no matches for this axis

        csv_path = os.path.join(arcpy.env.workspace, 'ec_subset_for_axis_{}.csv'.format(axis))

        if not matches:
            print 'No track matches axis %s' % axis
            self.create_empty_csv_export(csv_path)
        else:
            # select the matching measurements
            arcpy.management.SelectLayerByAttribute(self.measurements_fl,
                where_clause = SQL.or_(match.as_sql_clause() for match in matches))
            # export the matching measurements as CSV
            self.export_selected_measurements_to_csv(csv_path)

            # the field names to insert/request
            fnames =  ['SHAPE@XY'] + [fname for fname, ftype in fields]
            # the index of the track field
            track_idx = fnames.index('track')

            with arcpy.da.InsertCursor(nfc, fnames) as insert:
                # iterate over every track
                for match in matches:
                    track = str(match.track)
                    # iterate over every matching track interval
                    for idx, time in enumerate(match.matches):
                        where_clause = SQL.and_([SQL.eq_('track', track), time.as_sql_clause()])
                        new_track_name = '_'.join([track, str(idx)])
                        with arcpy.da.SearchCursor(fc, fnames, where_clause = where_clause) as rows:
                            for row in rows:
                                insert.insertRow([column if idx != track_idx else new_track_name for idx, column in enumerate(row)])

        self.add_axis_segment_association(axis)

        return nfc

    def add_axis_segment_association(self, axis):
        extracted_axis = os.path.join(self.fgdb, 'axis_%s' % axis)
        try:
            print 'Exporting axis %s' % axis
            # select the segments of this axis

            arcpy.management.AddField(self.axis_segment_fc, 'id', 'LONG')

            arcpy.management.SelectLayerByAttribute(self.axis_segment_fl,
                where_clause = SQL.eq_('Achsen_ID', SQL.quote_(axis)))
            arcpy.management.CopyFeatures(self.axis_segment_fl, extracted_axis)
            fc = self.measurements_by_axis_fc[axis]

            # calculate the neares segments
            arcpy.analysis.Near(in_features = fc, near_features = extracted_axis,
                location = 'NO_LOCATION', angle = 'NO_ANGLE', method = 'PLANAR')

            # replace the segment OID with the segment id
            arcpy.management.AddField(fc, 'segment', 'LONG')
            arcpy.management.AddField(fc, 'axis', 'TEXT')
            arcpy.management.MakeFeatureLayer(fc, 'ec_subset_fl')
            arcpy.management.MakeFeatureLayer(extracted_axis, 'extracted_axis_fl')
            try:
                oid_field_name = arcpy.Describe('extracted_axis_fl').OIDFieldName
                arcpy.management.AddJoin('ec_subset_fl', 'NEAR_FID', 'extracted_axis_fl', oid_field_name)
                arcpy.management.CalculateField('ec_subset_fl', 'segment', '!axis_{}.segment_id!'.format(axis), 'PYTHON')
                arcpy.management.CalculateField('ec_subset_fl', 'axis', "'{}'".format(axis), 'PYTHON')
            finally:
                arcpy.management.Delete('ec_subset_fl')
                arcpy.management.Delete('extracted_axis_fl')

            arcpy.management.DeleteField(fc, 'NEAR_FID')
            arcpy.management.DeleteField(fc, 'NEAR_DIST')



        finally:
            arcpy.management.Delete(extracted_axis)

    def create_axis_mbr_feature_class(self):
        out = os.path.join(self.fgdb, 'axis_mbr')
        arcpy.management.MinimumBoundingGeometry(
            in_features = self.node_fc,
            out_feature_class = out,
            geometry_type = 'ENVELOPE',
            group_option = 'LIST',
            group_field = 'AXIS')
        return out

    def get_tracks_for_axis_mbr(self, axis):
        # select only the MBR of the current axis
        arcpy.management.SelectLayerByAttribute(self.axis_mbr_fl, where_clause = SQL.eq_('AXIS', SQL.quote_(axis)))
        # select all measurements instersecting with the MBR
        arcpy.management.SelectLayerByLocation(self.measurements_fl, 'INTERSECT', self.axis_mbr_fl)
        # get the track ids of the intersecting measurements
        with arcpy.da.SearchCursor(self.measurements_fl, ['track'], sql_clause = ('DISTINCT', None)) as rows:
            return set(row[0] for row in rows)

    def create_node_feature_class(self):
        # create a the new feature class
        fc = arcpy.management.CreateFeatureclass(
            self.fgdb, 'nodes',
            geometry_type = 'POINT',
            spatial_reference = TrackMatcher.EPSG_4326)
        # and add the attribute definitions
        arcpy.management.AddField(fc, 'AXIS', 'TEXT')
        arcpy.management.AddField(fc, 'NODE_TYPE', 'SHORT')
        arcpy.management.AddField(fc, 'NODE_RANK', 'LONG')
        # lets fill in the features
        with arcpy.da.InsertCursor(fc, ['AXIS', 'NODE_TYPE', 'NODE_RANK', 'SHAPE@']) as ic:
            # the start nodes
            has_start = False
            with arcpy.da.SearchCursor(self.node_start_fc, ['Achsen_ID', 'SHAPE@']) as sc:
                for row in sc:
                    ic.insertRow((row[0], TrackMatcher.TYPE_START, 0, row[1]))

            # the start nodes of ranges of influence
            with arcpy.da.SearchCursor(self.node_influence_fc, ['Achsen_ID', 'SHAPE@', 'N_Rang']) as sc:
                for row in sc:
                    ic.insertRow((row[0], TrackMatcher.TYPE_INFLUENCE, 2 * (row[2] - 1) + 1, row[1]))
            # the traffic lights
            with arcpy.da.SearchCursor(self.node_lsa_fc, ['Achsen_ID', 'SHAPE@', 'K_Rang']) as sc:
                for row in sc:
                    ic.insertRow((row[0], TrackMatcher.TYPE_LSA, 2 * (row[2] - 1) + 2, row[1]))
        return fc

    def get_track_matches(self, track, axis, num_consecutive_results = 4):
        print 'checking track %s for axis %s' % (track, axis)
        arcpy.management.SelectLayerByAttribute(self.node_buffer_fl,
            where_clause = SQL.eq_('AXIS', SQL.quote_(axis)))
        node_range = xrange(1, count_features(self.node_buffer_fl))
        def get_node_matches(node): return self.get_node_matches(track, axis, node)
        node_matches = [match for node in node_range for match in get_node_matches(node)]
        return TrackMatchingResult(track, node_matches, num_consecutive_results)

    def get_node_matches(self, track, axis, node):
        arcpy.management.SelectLayerByAttribute(self.node_buffer_fl,
            where_clause = SQL.and_((SQL.eq_('AXIS', SQL.quote_(axis)),
                                     SQL.eq_('NODE_RANK', node))))
        assert count_features(self.node_buffer_fl) == 1
        arcpy.management.SelectLayerByAttribute(self.trajectories_fl,
            where_clause = SQL.eq_('track', track))
        assert count_features(self.trajectories_fl) > 0
        arcpy.management.SelectLayerByLocation(
            self.trajectories_fl,
            'INTERSECT',
            self.node_buffer_fl,
            selection_type = 'SUBSET_SELECTION')

        count = count_features(self.trajectories_fl)

        if count:
            min_time = None
            max_time = None
            threshold = timedelta(seconds=20)

            fields = ['start_time', 'end_time']
            sql_clause = (None, 'ORDER BY start_time')
            layer = self.trajectories_fl

            with arcpy.da.SearchCursor(layer, fields, sql_clause=sql_clause) as rows:
                for min, max in rows:
                    # first match
                    if min_time is None:
                        min_time, max_time = min, max
                    # if the delta is not not to big consider it a single match
                    elif (min - max_time) < threshold:
                        max_time = max
                    else:
                        yield NodeMatchingResult(min_time, max_time, node)
                        min_time = max_time = None

            if min_time is not None:
                yield NodeMatchingResult(min_time, max_time, node)


    def get_track_matches_for_axis(self, axis):
        tracks = self.get_tracks_for_axis_mbr(axis)
        print '%d tracks found for axis mbr %s' % (len(tracks), axis)
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
            ('id', 'id', identity),
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
            return arcpy.da.SearchCursor(self.measurements_fl, [f[1] for f in fields])
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

        def create_stop(): return Stop(axis=caxis, segment=csegment, track=ctrack,
                                       start=stop_start, stop=stop_end)

        with arcpy.da.SearchCursor(fc, fields, sql_clause = sql_clause) as rows:
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

class DatabaseParams(object):
    def __init__(self,
            database = 'envirocar',
            schema = 'public',
            username = 'postgres',
            password = 'postgres',
            hostname = 'localhost'):
        self.database = database
        self.schema = schema
        self.username = username
        self.password = password
        self.hostname = hostname
        self.path = None

    def create_sde(self, directory, name):
        if not os.path.exists(directory):
            os.makedirs(directory)
        arcpy.management.CreateDatabaseConnection(
            out_folder_path = directory,
            out_name = name,
            database_platform = 'POSTGRESQL',
            instance = self.hostname,
            account_authentication = 'DATABASE_AUTH',
            username = self.username,
            password = self.password,
            save_user_pass = True,
            database = self.database,
            schema = self.schema)

        self.path = os.path.join(directory, '%s.sde' % name)

        return self.path

    def get_feature_class(self, name):
        featureClassName = '.'.join([self.database, self.schema, name])
        return os.path.join(self.path, featureClassName)

def create_stop_table(fgdb, table_name, fc):
    field_names = ['axis', 'segment', 'track', 'start_time', 'end_time', 'duration']
    table = os.path.join(fgdb, table_name)
    if arcpy.Exists(table):
        arcpy.management.Delete(table)
    arcpy.management.CreateTable(fgdb, table_name)
    field_types = ['TEXT', 'LONG', 'TEXT', 'DATE', 'DATE', 'LONG']
    for field_name, field_type in zip(field_names, field_types):
        arcpy.management.AddField(table, field_name, field_type)

    with arcpy.da.InsertCursor(table, field_names) as insert:
        for stop in Stop.find(fc, stop_start_threshold=5, stop_end_threshold=10):
            duration = long(stop.duration.total_seconds() * 1000)
            insert.insertRow((stop.axis, stop.segment, stop.track, stop.start, stop.stop, duration))

def axis(range):
    for axis in range:
        yield '{}_1'.format(axis)
        yield '{}_2'.format(axis)

def nwise(iterable, n = 2):
    return izip(*[islice(it, idx, None) for idx, it in enumerate(tee(iterable, n))])

def first(iterable, default = None):
    if iterable:
        for elem in iterable:
            return elem
    return default

def count_features(layer):
    return int(arcpy.management.GetCount(layer).getOutput(0))

class SQL(object):
    @staticmethod
    def is_between_(name, value):
        return '(%s BETWEEN \'%s\' AND \'%s\')' % (name, value[0], value[1])

    @staticmethod
    def is_null_(name):
        return '%s IS NULL' % name

    @staticmethod
    def eq_(name, value):
        return '%s = %s' % (name, str(value))

    @staticmethod
    def quote_(value):
        return '\'%s\'' % value

    @staticmethod
    def and_(iterable):
        return ' AND '.join('(%s)' % str(x) for x in iterable)

    @staticmethod
    def or_(iterable):
        return ' OR '.join('(%s)' % str(x) for x in iterable)

def get_field_type(table, field_name):
    for field in arcpy.ListFields(table):
        if field.name == field_name:
            return field.type
    return None

def rename_field(table, old_name, new_name):
    arcpy.management.AddField(table, new_name, get_field_type(table, old_name))
    arcpy.management.CalculateField(table, new_name, '!{}!'.format(old_name), 'PYTHON')
    arcpy.management.DeleteField(table, old_name)

def merge_feature_classes(feature_classes, target, delete=True):
    iterator = iter(feature_classes)
    if arcpy.Exists(target):
        arcpy.management.Delete(target)
    if delete:
        arcpy.management.Rename(iterator.next(), target)
    else:
        arcpy.management.Copy(iterator.next(), target)
    for subset in iterator:
        arcpy.management.Append(subset, target)
        if delete:
            arcpy.management.Delete(subset)

def create_tracks(in_fc, out_fc_location, out_fc_name):
    def _as_polyline(coordinates):
        points = (arcpy.Point(*c) for c in coordinates)
        return arcpy.Polyline(arcpy.Array(points))

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

    arcpy.management.CreateFeatureclass(
        out_path=out_fc_location,
        out_name=out_fc_name,
        geometry_type='POLYLINE',
        spatial_reference=arcpy.SpatialReference(4326))

    out_fc = os.path.join(out_fc_location, out_fc_name)

    arcpy.management.AddField(out_fc, 'axis', 'TEXT')
    arcpy.management.AddField(out_fc, 'track', 'TEXT')
    arcpy.management.AddField(out_fc, 'start_time', 'DATE')
    arcpy.management.AddField(out_fc, 'stop_time', 'DATE')



    output_fields = ['SHAPE@', 'axis', 'track', 'start_time', 'stop_time']
    input_fields = ['SHAPE@XY', 'axis', 'track', 'time']
    sql_clause = (None, 'ORDER BY axis, track, time')

    with arcpy.da.InsertCursor(out_fc, output_fields) as insert:
        with arcpy.da.SearchCursor(in_fc, input_fields, sql_clause=sql_clause) as rows:
            for polyline in _create_polylines(rows):
                insert.insertRow(polyline)

def calculate_statistics(measurements_fc, stops_table, out_location):
    def stats(case_fields, out_name):
        out_table = os.path.join(out_location, out_name)
        arcpy.analysis.Statistics(
            in_table=measurements_fc,
            out_table=out_table,
            statistics_fields=[
                ('co2', 'MEAN'),
                ('consumption','MEAN')
            ],
            case_field=case_fields)
        rename_field(out_table, 'FREQUENCY', 'num_observations')
        rename_field(out_table, 'MEAN_co2', 'co2')
        rename_field(out_table, 'MEAN_consumption', 'consumption')
        add_join_field(out_table, case_fields)

    def stop_stats(case_fields, out_name):
        out_table = os.path.join(out_location, out_name)
        arcpy.analysis.Statistics(
            in_table=stops_table,
            out_table=out_table,
            statistics_fields=[('duration', 'MEAN')],
            case_field=case_fields)
        rename_field(out_table, 'FREQUENCY', 'num_stops')
        rename_field(out_table, 'MEAN_duration', 'duration')
        add_join_field(out_table, case_fields)

    stats(['axis'], 'per_axis')
    stats(['axis', 'segment'], 'per_segment')
    stats(['axis', 'track'], 'per_axis_per_track')
    stats(['axis', 'track', 'segment'], 'per_segment_per_track')

    stop_stats(['axis'], 'stops_per_axis')
    stop_stats(['axis', 'segment'], 'stops_per_segment')
    stop_stats(['axis', 'track'], 'stops_per_axis_per_track')
    stop_stats(['axis', 'track', 'segment'], 'stops_per_segment_per_track')

def get_distinct_values(feature_class, attribute):
    with arcpy.da.SearchCursor(feature_class, (attribute), sql_clause=('DISTINCT', None)) as rows:
        return sorted(set(row[0] for row in rows))

def add_join_field(feature_class, fields, field_name='join_field'):
    arcpy.management.AddField(feature_class, field_name, 'TEXT')
    fields = ', '.join('!{}!'.format(field) for field in fields)
    expression = '"|".join(str(x) for x in ({}))'.format(fields)
    arcpy.management.CalculateField(feature_class, field_name,
        expression=expression, expression_type='PYTHON')

def convert_null(table, field, value=0):
    arcpy.management.CalculateField(in_table=table, field=field,
        expression='!{0}! if !{0}! is not None else {1}'.format(field, value),
        expression_type='PYTHON')

def find_passages_per_segment(fgdb, axis_track_segment, axis_segment):
    passages_without_stops = os.path.join(fgdb, 'passages_without_stops')
    passages_with_stops = os.path.join(fgdb, 'passages_with_stops')
    try:
        arcpy.management.MakeTableView(axis_track_segment, 'passages_per_segment')
        try:
            arcpy.management.AddJoin('passages_per_segment', in_field='join_field',
                join_table=os.path.join(fgdb, 'stops_per_segment_per_track'),
                join_field='join_field', join_type='KEEP_ALL')

            # select all passages that have no stop
            arcpy.management.SelectLayerByAttribute('passages_per_segment',
                where_clause=SQL.or_((SQL.is_null_('stops_per_segment_per_track.num_stops'),
                                      SQL.eq_('stops_per_segment_per_track.num_stops', 0))))

            arcpy.analysis.Statistics(
                in_table='passages_per_segment',
                out_table=passages_without_stops,
                statistics_fields=[('axis_track_segment.track', 'COUNT')],
                case_field=('axis_track_segment.axis', 'axis_track_segment.segment'))
            rename_field(passages_without_stops, 'axis_track_segment_axis', 'axis')
            rename_field(passages_without_stops, 'axis_track_segment_segment', 'segment')
            rename_field(passages_without_stops, 'FREQUENCY', 'passages_without_stops')
            arcpy.management.DeleteField(passages_without_stops, 'COUNT_axis_track_segment_track')

            # select all passages that have stops
            arcpy.management.SelectLayerByAttribute('passages_per_segment',
                where_clause='stops_per_segment_per_track.num_stops >= 0')

            arcpy.analysis.Statistics(
                in_table='passages_per_segment',
                out_table=passages_with_stops,
                statistics_fields=[('axis_track_segment.track', 'COUNT')],
                case_field=('axis_track_segment.axis', 'axis_track_segment.segment'))
            rename_field(passages_with_stops, 'axis_track_segment_axis', 'axis')
            rename_field(passages_with_stops, 'axis_track_segment_segment', 'segment')
            rename_field(passages_with_stops, 'FREQUENCY', 'passages_with_stops')
            arcpy.management.DeleteField(passages_with_stops, 'COUNT_axis_track_segment_track')

        finally:
            arcpy.management.Delete('passages_per_segment')

        arcpy.management.MakeTableView(axis_segment, 'axis_segment')
        try:

            arcpy.management.AddJoin('axis_segment', 'segment', passages_with_stops, 'segment')
            arcpy.management.AddJoin('axis_segment', 'segment', passages_without_stops, 'segment')

            fms = arcpy.FieldMappings()
            fms.addTable('axis_segment')

            fields = ('passages_with_stops_OBJECTID',
                      'passages_with_stops_axis',
                      'passages_with_stops_segment',
                      'passages_without_stops_OBJECTID',
                      'passages_without_stops_axis',
                      'passages_without_stops_segment')

            for field in fields:
                fms.removeFieldMap(fms.findFieldMapIndex(field))

            passages_per_segment = os.path.join(fgdb, 'passages_per_segment')

            arcpy.conversion.TableToTable(in_rows='axis_segment',
                out_path=fgdb,  out_name='passages_per_segment', field_mapping=fms)

            rename_field(passages_per_segment,
                'passages_with_stops_passages_with_stops',
                'passages_with_stops')
            rename_field(passages_per_segment,
                'passages_without_stops_passages_without_stops',
                'passages_without_stops')

            arcpy.management.AddField(passages_per_segment, 'passages_overall', 'LONG')

            convert_null(passages_per_segment, 'passages_with_stops', 0)
            convert_null(passages_per_segment, 'passages_without_stops', 0)
            arcpy.management.CalculateField(passages_per_segment, 'passages_overall',
                expression='!passages_with_stops! + !passages_without_stops!',
                expression_type='PYTHON')

        finally:
            arcpy.management.Delete('axis_segment')
    finally:
        pass
        #if arcpy.Exists(passages_with_stops):
            #arcpy.management.Delete(passages_with_stops)
        #if arcpy.Exists(passages_without_stops):
            #arcpy.management.Delete(passages_without_stops)

def create_axis_track_segment_table(fgdb, measurements_fc):
    """Creates a table containing all axis/track/segment combinations."""
    axis_track_segment = os.path.join(fgdb, 'axis_track_segment')
    arcpy.analysis.Statistics(
        in_table=measurements_fc,
        out_table=axis_track_segment,
        statistics_fields=[('id', 'COUNT')],
        case_field=('axis','track','segment'))
    arcpy.management.DeleteField(axis_track_segment, 'COUNT_id')
    arcpy.management.DeleteField(axis_track_segment, 'FREQUENCY')
    add_join_field(axis_track_segment, ['axis', 'track', 'segment'])
    return axis_track_segment

def create_axis_segment_table(fgdb, axis_segment_fc):
    """Creates a table all axis/segment combinatinons."""
    axis_segment = os.path.join(fgdb, 'axis_segment')
    arcpy.analysis.Statistics(
        in_table=axis_segment_fc,
        out_table=axis_segment,
        statistics_fields=[('segment_id', 'COUNT')],
        case_field=('Achsen_ID', 'segment_id'))
    rename_field(axis_segment, 'Achsen_ID', 'axis')
    rename_field(axis_segment, 'segment_id', 'segment')
    arcpy.management.DeleteField(axis_segment, 'FREQUENCY')
    arcpy.management.DeleteField(axis_segment, 'COUNT_segment_id')
    return axis_segment

def create_segments_per_track_table(fgdb, axis_track_segment):
    """Creates a table containing the number of segments per track."""
    num_segments_per_track = os.path.join(fgdb, 'num_segments_per_track')
    arcpy.analysis.Statistics(
        in_table=axis_track_segment,
        out_table=num_segments_per_track,
        statistics_fields=[('segment', 'COUNT')],
        case_field=('axis', 'track'))

    rename_field(num_segments_per_track, 'FREQUENCY', 'segments')
    arcpy.management.DeleteField(num_segments_per_track, 'COUNT_segment')
    add_join_field(num_segments_per_track, ['axis','track'])
    return num_segments_per_track

def create_segments_per_axis_table(fgdb, axis_segment_fc):
    """Creates a table containing the number of segments per axis."""
    num_segments_per_axis = os.path.join(fgdb, 'num_segments_per_axis')
    arcpy.analysis.Statistics(
        in_table=axis_segment_fc,
        out_table=num_segments_per_axis,
        statistics_fields=[('segment_id', 'COUNT')],
        case_field=('Achsen_ID'))
    rename_field(num_segments_per_axis, 'Achsen_ID', 'axis')
    rename_field(num_segments_per_axis, 'FREQUENCY', 'segments')
    arcpy.management.DeleteField(num_segments_per_axis, 'COUNT_segment_id')
    return num_segments_per_axis

def find_passages_per_axis(fgdb):
    pass

def find_passages(fgdb, axis_model, measurements_fc):
    axis_segment = create_axis_segment_table(fgdb, axis_model.axis_segment_fc)
    axis_track_segment = create_axis_track_segment_table(fgdb, measurements_fc)
    try:
        num_segments_per_track = create_segments_per_track_table(fgdb, axis_track_segment)
        num_segments_per_axis = create_segments_per_axis_table(fgdb, axis_model.axis_segment_fc)
        find_passages_per_axis(fgdb)
        find_passages_per_segment(fgdb, axis_track_segment, axis_segment)
    finally:
        pass
        #arcpy.management.Delete(axis_track_segment)
        #arcpy.management.Delete(axis_segment)
