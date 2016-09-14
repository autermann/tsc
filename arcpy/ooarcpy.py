import arcpy
import os
import textwrap
import logging

from abc import ABCMeta, abstractmethod, abstractproperty

log = logging.getLogger(__name__)

def debug(command, param):
    if log.isEnabledFor(logging.DEBUG):
        log.debug('%s%s', command, str(param))

class ArcpyDatabase(object):
    __metaclass__ = ABCMeta
    @abstractproperty
    def id(self): pass

    def delete_if_exists(self):
        if self.exists():
            self.delete()

    def create_if_not_exists(self):
        if not self.exists():
            self.create()

    def exists(self):
        debug('arcpy.Exists', (self.id,))
        return arcpy.Exists(self.id)

    def delete(self):
        debug('arcpy.management.Delete', (self.id,))
        arcpy.management.Delete(self.id)

    @abstractmethod
    def feature_class(self, name): pass

    @abstractmethod
    def table(self, name): pass

    def __str__(self):
        return self.id

class SDE(ArcpyDatabase):
    def __init__(self, path,
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
        self.path = path

    @property
    def id(self):
        return self.path

    def create(self):
        path, name = os.path.split(self.path)

        debug('arcpy.management.CreateDatabaseConnection',
            (path, name, 'POSTGRESQL', self.hostname, 'DATABASE_AUTH',
            self.username, self.password, True, self.database, self.schema))
        arcpy.management.CreateDatabaseConnection(
            out_folder_path = path,
            out_name = name,
            database_platform = 'POSTGRESQL',
            instance = self.hostname,
            account_authentication = 'DATABASE_AUTH',
            username = self.username,
            password = self.password,
            save_user_pass = True,
            database = self.database,
            schema = self.schema)

    def feature_class(self, name):
        featureClassName = '.'.join([self.database, self.schema, name])
        return FeatureClass(os.path.join(self.path, featureClassName))

    def table(self, name):
        tableName = '.'.join([self.database, self.schema, name])
        return Table(os.path.join(self.path, tableName))

class FileGDB(ArcpyDatabase):
    def __init__(self, path):
        self.path = path

    @property
    def id(self):
        return self.path

    def create(self):
        path, name = os.path.split(self.path)
        debug('arcpy.management.CreateFileGDB', (path, name))
        arcpy.management.CreateFileGDB(path, name)

    def feature_class(self, name):
        return FeatureClass(os.path.join(self.path, name))

    def table(self, name):
        return Table(os.path.join(self.path, name))

class ArcPyEntityBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, id):
        self.id = id

    def delete(self):
        debug('arcpy.management.Delete', (self.id,))
        arcpy.management.Delete(self.id)

    @property
    def oid_field_name(self):
        return self.describe().OIDFieldName

    def describe(self):
        debug('arcpy.Describe', (self.id,))
        return arcpy.Describe(self.id)

    def delete_if_exists(self):
        if self.exists():
            self.delete()

    def exists(self):
        debug('arcpy.Exists', (self.id,))
        return arcpy.Exists(self.id)

    def add_field(self, name, type):
        debug('arcpy.management.AddField', (self.id, name, type))
        arcpy.management.AddField(self.id, name, type)

    def delete_field(self, name):
        debug('arcpy.management.DeleteField', (self.id, name))
        arcpy.management.DeleteField(self.id, name)

    def field_exist(self, name):
        for field in self.list_fields():
            if field.name == name:
                return True
        return False

    def calculate_field(self, field, expression, expression_type='PYTHON_9.3', code_block=None):
        debug('arcpy.management.CalculateField', (self.id, field, expression, expression_type, code_block))
        arcpy.management.CalculateField(self.id, field, expression, expression_type, code_block)

    def set_field_if_null(self, field, value):
        self.calculate_field(field, '!{0}! if !{0}! is not None else {1}'.format(field, value))

    def rename_field(self, old_name, new_name):
        debug('arcpy.management.AlterField', (self.id, old_name, new_name))
        arcpy.management.AlterField(self.id, old_name, new_name)
        #self.add_field(new_name, self.get_field_type(old_name))
        #self.calculate_field(new_name, '!{}!'.format(old_name))
        #self.delete_field(old_name)

    def list_fields(self):
        return arcpy.ListFields(self.id)

    def get_field_type(self, field_name):
        for field in self.list_fields():
            if field.name == field_name:
                return field.type
        return None

    def search(self, field_names='*', where_clause=None, spatial_reference=None, explode_to_points=False, sql_clause=(None, None)):
        debug('arcpy.da.SearchCursor', (self.id, field_names, where_clause, spatial_reference, explode_to_points, sql_clause))
        return arcpy.da.SearchCursor(self.id, field_names, where_clause, spatial_reference, explode_to_points, sql_clause)

    def insert(self, field_names='*'):
        debug('arcpy.da.InsertCursor', (self.id, field_names))
        return arcpy.da.InsertCursor(self.id, field_names)

    def update(self, field_names, where_clause=None, spatial_reference=None, explode_to_points=False, sql_clause=(None,None)):
        debug('arcpy.da.UpdateCursor', (self.id, field_names, where_clause, spatial_reference, explode_to_points, sql_clause))
        return arcpy.da.UpdateCursor(self.id, field_names, where_clause, spatial_reference, explode_to_points, sql_clause)

    def statistics(self, out_table, statistics_fields, case_field=None):
        try:
            out_table = out_table.id
        except AttributeError:
            pass
        debug('arcpy.analysis.Statistics', (self.id, out_table, statistics_fields, case_field))
        arcpy.analysis.Statistics(self.id, out_table, statistics_fields, case_field)
        return Table(out_table)

    def count(self):
        #debug('arcpy.management.GetCount', (self.id,))
        return int(arcpy.management.GetCount(self.id).getOutput(0))

    def __len__(self):
        return self.count()

    def add_join_field(self, fields, field_name='join_field'):
        self.add_field(field_name, 'TEXT')
        fields = ', '.join('!{}!'.format(field) for field in fields)
        expression = '"|".join(str(x) for x in ({}))'.format(fields)
        self.calculate_field(field_name, expression)
        debug('arcpy.management.AddIndex', (self.id, field_name, field_name + '_idx'))
        self.add_index(field_name, field_name + '_idx')

    def add_index(self, fields, index_name, unique=False, ascending=False):
        unique = 'UNIQUE' if unique else 'NON_UNIQUE'
        ascending = 'ASCENDING' if ascending else 'NON_ASCENDING'
        debug('arcpy.management.AddIndex', (self.id, fields, index_name, unique, ascending))
        arcpy.management.AddIndex(self.id, fields=fields, index_name=index_name, unique=unique, ascending=ascending)

    def list_indexes(self, wild_card=None):
        return arcpy.ListIndexes(self.id, wild_card=wild_card)

    def get_distinct_values(self, attribute):
        with self.search((attribute), sql_clause=('DISTINCT', None)) as rows:
            return sorted(set(row[0] for row in rows))

    def add_id_field(self, field_name='id'):
        if self.field_exist(field_name):
            self.delete_field(field_name)
        self.add_field(field_name, 'LONG')
        code_block = textwrap.dedent("""\
        id = 0
        def autoIncrement():
            global id
            id += 1
            return id
        """)
        self.calculate_field(field_name, 'autoIncrement()', code_block=code_block)
        return self.path

    def __str__(self):
        return self.id

class ArcPyEntity(ArcPyEntityBase):
    __metaclass__ = ABCMeta

    def __init__(self, path):
        super(ArcPyEntity, self).__init__(path)

    @property
    def path(self):
        return self.id

    @abstractmethod
    def view(self, name):
        pass

    def rename(self, path):
        try:
            path = path.id
        except AttributeError:
            pass
        debug('arcpy.management.Rename', (self.id, path))
        arcpy.management.Rename(self.id, path)
        self.id = path


    def append_to(self, target):
        target.append(self)

    def append(self, source):
        debug('arcpy.management.Append', (source.id, self.id))
        arcpy.management.Append(source.id, self.id)

    def copy(self, target):
        try:
            target = target.id
        except AttributeError:
            pass
        debug('arcpy.management.Copy', (self.id, target))
        arcpy.management.Copy(self.id, target)
        return self.__class__(target)

class ArcPyEntityView(ArcPyEntityBase):
    __metaclass__ = ABCMeta

    def __init__(self, name=None, source=None):

        self.source = source

        if source is not None:
            if name is None:
                name = arcpy.Describe(source.id).name
            self.create(source, name)

        super(ArcPyEntityView, self).__init__(name)

    @property
    def name(self):
        return self.id

    @abstractmethod
    def create(self, source, name):
        pass

    def add_join(self, field, other, other_field, keep_all=True):
        join_type = 'KEEP_ALL' if keep_all else 'KEEP_COMMON'
        debug('arcpy.management.AddJoin', (self.id, field, other.id, other_field, join_type))
        arcpy.management.AddJoin(self.id, field, other.id, other_field, join_type)

    def _select_by_attribute(self, selection_type='NEW_SELECTION', where_clause=None):
        debug('arcpy.management.SelectLayerByAttribute', (self.id, selection_type, where_clause))
        arcpy.management.SelectLayerByAttribute(self.id, selection_type, where_clause)

    def new_selection(self, where_clause, invert_where_clause=False):
        self._select_by_attribute('NEW_SELECTION', where_clause)

    def add_to_selection(self, where_clause, invert_where_clause=False):
        self._select_by_attribute('ADD_TO_SELECTION', where_clause)

    def remove_from_selection(self, where_clause, invert_where_clause=False):
        self._select_by_attribute('REMOVE_FROM_SELECTION', where_clause)

    def subset_selection(self, where_clause, invert_where_clause=False):
        self._select_by_attribute('SUBSET_SELECTION', where_clause)

    def switch_selection(self):
        self._select_by_attribute('SWITCH_SELECTION')

    def clear_selection(self):
        self._select_by_attribute('CLEAR_SELECTION')

class SpatialArcPyEntityBase(ArcPyEntityBase):

    @property
    def shape_field_name(self):
        return self.describe().shapeFieldName

    def buffer(self, out_feature_class, buffer_distance_or_field, line_side='FULL', line_end_type='ROUND', dissolve_option='NONE', dissolve_field=None, method='PLANAR'):
        try:
            out_feature_class = out_feature_class.id
        except AttributeError:
            pass

        debug('arcpy.analysis.Buffer', (self.id, out_feature_class, buffer_distance_or_field, line_side, line_end_type, dissolve_option, dissolve_field, method))
        arcpy.analysis.Buffer(self.id, out_feature_class, buffer_distance_or_field, line_side, line_end_type, dissolve_option, dissolve_field, method)
        return FeatureClass(out_feature_class)

    def copy_features(self, out_feature_class):

        try:
            out_feature_class = out_feature_class.id
        except AttributeError:
            pass
        debug('arcpy.management.CopyFeatures', (self.id, out_feature_class))
        arcpy.management.CopyFeatures(self.id, out_feature_class)
        return FeatureClass(out_feature_class)

    def near(self, near_features, search_radius=None, location=False, angle=False, method='PLANAR'):
        location = 'LOCATION' if location else 'NO_LOCATION'
        angle = 'ANGLE' if angle else 'NO_ANGLE'
        debug('arcpy.analysis.Near', (self.id, near_features.id, search_radius, location, angle, method))
        arcpy.analysis.Near(self.id, near_features.id, search_radius, location, angle, method)

    def minimum_bounding_geometry(self, out_feature_class, geometry_type='ENVELOPE', group_option='NONE', group_field=None, mbg_fields_option=False):
        mbg_fields_option = 'MBG_FIELDS' if mbg_fields_option else 'NO_MBG_FIELDS'
        try:
            out_feature_class = out_feature_class.id
        except AttributeError:
            pass
        debug('arcpy.management.MinimumBoundingGeometry', (self.id, out_feature_class, geometry_type, group_option, group_field, mbg_fields_option))
        arcpy.management.MinimumBoundingGeometry(self.id, out_feature_class, geometry_type, group_option, group_field, mbg_fields_option)
        return FeatureClass(out_feature_class)

    def to_feature_class(self, out_feature_class, where_clause=None, field_mapping=None):
        try:
            out_feature_class = out_feature_class.id
        except AttributeError:
            pass
        out_path, out_name = os.path.split(out_feature_class)
        debug('arcpy.conversion.FeatureClassToFeatureClass', (self.id, out_path,  out_name, where_clause, field_mapping))
        arcpy.conversion.FeatureClassToFeatureClass(self.id, out_path,  out_name, where_clause, field_mapping)

class FeatureClass(ArcPyEntity, SpatialArcPyEntityBase):
    def view(self, name=None):
        return FeatureLayer(source=self, name=name)

    def create(self, geometry_type=None, template=None, has_m=None, has_z=None, spatial_reference=None):
        out_path, out_name = os.path.split(self.id)
        debug('arcpy.management.CreateFeatureclass', (out_path, out_name, geometry_type, template, has_m, has_z, spatial_reference))
        arcpy.management.CreateFeatureclass(out_path, out_name, geometry_type, template, has_m, has_z, spatial_reference)

class FeatureLayer(ArcPyEntityView, SpatialArcPyEntityBase):
    def create(self, source, name):
        debug('arcpy.management.MakeFeatureLayer', (source.id, name))
        arcpy.management.MakeFeatureLayer(source.id, name)

    def _select_by_location(self, overlap_type='INTERSECT', select_features=None, search_distance='#', selection_type='NEW_SELECTION', invert_spatial_relationship=False):
        invert_spatial_relationship = 'INVERT' if invert_spatial_relationship else 'NOT_INVERT'
        debug('arcpy.management.SelectLayerByLocation', (self.id, overlap_type, select_features.id, search_distance, selection_type, invert_spatial_relationship))
        arcpy.management.SelectLayerByLocation(in_layer=self.id, overlap_type=overlap_type, select_features=select_features.id, search_distance=search_distance, selection_type=selection_type, invert_spatial_relationship=invert_spatial_relationship)

    def new_selection_by_location(self, features, overlap_type='INTERSECT', search_distance='#', invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'NEW_SELECTION', invert_spatial_relationship)

    def add_to_selection_by_location(self, features, overlap_type='INTERSECT', search_distance='#', invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'ADD_TO_SELECTION', invert_spatial_relationship)

    def remove_from_selection_by_location(self, features, overlap_type='INTERSECT', search_distance='#', invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'REMOVE_FROM_SELECTION', invert_spatial_relationship)

    def subset_selection_by_location(self, features, overlap_type='INTERSECT', search_distance='#', invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'SUBSET_SELECTION', invert_spatial_relationship)

class TableLikeArcPyEntityBase(ArcPyEntityBase):
    __metaclass__ = ABCMeta

    def to_table(self, out_table, where_clause=None, field_mapping=None):
        try:
            out_table = out_table.id
        except AttributeError:
            pass
        out_path, out_name = os.path.split(out_table)
        debug('arcpy.conversion.TableToTable', (self.id, out_path,  out_name, where_clause, field_mapping))
        arcpy.conversion.TableToTable(self.id, out_path,  out_name, where_clause, field_mapping)

class Table(ArcPyEntity, TableLikeArcPyEntityBase):
    def view(self, name=None):
        return TableView(source=self, name=name)

    def create(self):
        out_path, out_name = os.path.split(self.id)
        debug('arcpy.management.CreateTable', (out_path, out_name))
        arcpy.management.CreateTable(out_path, out_name)

class TableView(ArcPyEntityView, TableLikeArcPyEntityBase):
    def create(self, source, name):
        debug('arcpy.management.MakeTableView', (source.id, name))
        arcpy.management.MakeTableView(source.id, name)

