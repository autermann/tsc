import arcpy
import os
from abc import ABCMeta, abstractmethod, abstractproperty

class FileGDB(object):

    def __init__(self, path):
        self.id = path

    @property
    def path(self):
        return self.id

    def create(self):
        path, name = os.path.split(self.path)
        arcpy.management.CreateFileGDB(path, name)

    def delete_if_exists(self):
        if self.exists():
            self.delete()

    def create_if_not_exists(self):
        if not self.exists():
            self.create()

    def exists(self):
        return arcpy.Exists(self.path)

    def feature_class(self, name):
        return FeatureClass(os.path.join(self.path, name))

    def table(self, name):
        return Table(os.path.join(self.path, name))

class SDE(object):
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

    def create_if_not_exists(self, path):
        directory, name = os.path.split(path)
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

        #self.path = os.path.join(directory, '%s.sde' % name)

        self.path = path

    def feature_class(self, name):
        featureClassName = '.'.join([self.database, self.schema, name])
        return FeatureClass(os.path.join(self.path, featureClassName))

class ArcPyEntityBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, id):
        self.id = id

    def delete(self):
        arcpy.management.Delete(self.id)

    @property
    def oid_field_name(self):
        return self.describe().OIDFieldName

    def describe(self):
        return arcpy.Describe(self.id)

    def delete_if_exists(self):
        if self.exists():
            self.delete()

    def exists(self):
        return arcpy.Exists(self.id)

    def add_field(self, name, type):
        arcpy.management.AddField(self.id, name, type)

    def delete_field(self, name):
        arcpy.management.DeleteField(self.id, name)

    def calculate_field(self, field, expression, expression_type='PYTHON_9.3', code_block=None):
        arcpy.management.CalculateField(self.id, field, expression, expression_type, code_block)

    def set_field_if_null(self, field, value):
        self.calculate_field(field, '!{0}! if !{0}! is not None else {1}'.format(field, value))

    def rename_field(self, old_name, new_name):
        self.add_field(new_name, self.get_field_type(old_name))
        self.calculate_field(new_name, '!{}!'.format(old_name))
        self.delete_field(old_name)

    def list_fields(self):
        return arcpy.ListFields(self.id)

    def get_field_type(self, field_name):
        for field in self.list_fields():
            if field.name == field_name:
                return field.type
        return None

    def search(self, field_names='*', where_clause=None, spatial_reference=None, explode_to_points=False, sql_clause=(None, None)):
        return arcpy.da.SearchCursor(self.id, field_names, where_clause, spatial_reference, explode_to_points, sql_clause)

    def insert(self, field_names='*'):
        return arcpy.da.InsertCursor(self.id, field_names)

    def update(self, field_names, where_clause=None, spatial_reference=None, explode_to_points=False, sql_clause=(None,None)):
        return arcpy.da.UpdateCursor(self.id, field_names, where_clause, spatial_reference, explode_to_points, sql_clause)

    def statistics(self, out_table, statistics_fields, case_field=None):
        try:
            out_table = out_table.id
        except AttributeError:
            pass
        arcpy.analysis.Statistics(self.id, out_table, statistics_fields, case_field)
        return Table(out_table)

    def count(self):
        return int(arcpy.management.GetCount(self.id).getOutput(0))

    def __len__(self):
        return self.count()

    def add_join_field(self, fields, field_name='join_field'):
        self.add_field(field_name, 'TEXT')
        fields = ', '.join('!{}!'.format(field) for field in fields)
        expression = '"|".join(str(x) for x in ({}))'.format(fields)
        self.calculate_field(field_name, expression)

    def get_distinct_values(self, attribute):
        with self.search((attribute), sql_clause=('DISTINCT', None)) as rows:
            return sorted(set(row[0] for row in rows))


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
        arcpy.management.Rename(self.id, path)
        self.id = path

    def copy(self, path):
        arcpy.management.Copy(self.id, path)

    def append_to(self, target):
        target.append(self)

    def append(self, source):
        arcpy.management.Append(source.id, self.id)

    def copy(self, target):
        try:
            target = target.id
        except AttributeError:
            pass
        arcpy.management.Copy(self.id, target)
        return self.__class__(target)

class ArcPyEntityView(ArcPyEntityBase):
    __metaclass__ = ABCMeta

    def __init__(self, name, source=None):
        super(ArcPyEntityView, self).__init__(name)
        self.source = source

        if source is not None:
            self.create(source, name)

    @property
    def name(self):
        return self.id

    @abstractmethod
    def create(self, source, name):
        pass

    def add_join(self, field, other, other_field, keep_all=True):
        join_type = 'KEEP_ALL' if keep_all else 'KEEP_COMMON'
        arcpy.management.AddJoin(self.id, field, other.id, other_field, join_type)

    def _select_by_attribute(self, selection_type='NEW_SELECTION', where_clause=None):
        #invert_where_clause = 'INVERT' if invert_where_clause else 'NON_INVERT'
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
    def buffer(self, out_feature_class, buffer_distance_or_field, line_side='FULL', line_end_type='ROUND', dissolve_option='NONE', dissolve_field=None, method='PLANAR'):

        try:
            out_feature_class = out_feature_class.id
        except AttributeError:
            pass

        arcpy.analysis.Buffer(self.id, out_feature_class, buffer_distance_or_field, line_side, line_end_type, dissolve_option, dissolve_field, method)
        return FeatureClass(out_feature_class)

    def copy_features(self, out_feature_class):

        try:
            out_feature_class = out_feature_class.id
        except AttributeError:
            pass
        arcpy.management.CopyFeatures(self.id, out_feature_class)

        return FeatureClass(out_feature_class)

    def near(self, near_features, search_radius=None, location=False, angle=False, method='PLANAR'):
        location = 'LOCATION' if location else 'NO_LOCATION'
        angle = 'ANGLE' if angle else 'NO_ANGLE'
        arcpy.analysis.Near(self.id, near_features.id, search_radius, location, angle, method)

    def minimum_bounding_geometry(self, out_feature_class, geometry_type='ENVELOPE', group_option='NONE', group_field=None, mbg_fields_option=False):
        mbg_fields_option = 'MBG_FIELDS' if mbg_fields_option else 'NO_MBG_FIELDS'
        try:
            out_feature_class = out_feature_class.id
        except AttributeError:
            pass
        arcpy.management.MinimumBoundingGeometry(self.id, out_feature_class, geometry_type, group_option, group_field, mbg_fields_option)
        return FeatureClass(out_feature_class)

class FeatureClass(ArcPyEntity, SpatialArcPyEntityBase):
    def view(self, name):
        return FeatureLayer(source=self, name=name)

    def create(self, geometry_type=None, template=None, has_m=None, has_z=None, spatial_reference=None):
        out_path, out_name = os.path.split(self.id)
        arcpy.management.CreateFeatureclass(out_path, out_name, geometry_type, template, has_m, has_z, spatial_reference)

class FeatureLayer(ArcPyEntityView, SpatialArcPyEntityBase):
    def create(self, source, name):
        arcpy.management.MakeFeatureLayer(source.id, name)

    def _select_by_location(self, overlap_type='INTERSECT', select_features=None, search_distance=None, selection_type='NEW_SELECTION', invert_spatial_relationship=False):
        invert_spatial_relationship = 'INVERT' if invert_spatial_relationship else 'NOT_INVERT'
        arcpy.management.SelectLayerByLocation(self.id, overlap_type, select_features.id, search_distance, selection_type, invert_spatial_relationship)

    def new_selection_by_location(self, features, overlap_type='INTERSECT', search_distance=None, invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'NEW_SELECTION', invert_spatial_relationship)

    def add_to_selection_by_location(self, features, overlap_type='INTERSECT', search_distance=None, invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'ADD_TO_SELECTION', invert_spatial_relationship)

    def remove_from_selection_by_location(self, features, overlap_type='INTERSECT', search_distance=None, invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'REMOVE_FROM_SELECTION', invert_spatial_relationship)

    def subset_selection_by_location(self, features, overlap_type='INTERSECT', search_distance=None, invert_spatial_relationship=False):
        self._select_by_location(overlap_type, features, search_distance, 'SUBSET_SELECTION', invert_spatial_relationship)

class TableLikeArcPyEntityBase(ArcPyEntityBase):
    __metaclass__ = ABCMeta

    def to_table(self, out_table, field_mapping=None):
        try:
            out_table = out_table.id
        except AttributeError:
            pass
        out_path, out_name = os.path.split(out_table)
        arcpy.conversion.TableToTable(self.id, out_path=out_path,  out_name=out_name, field_mapping=field_mapping)


class Table(ArcPyEntity, TableLikeArcPyEntityBase):
    def view(self, name):
        return TableView(source=self, name=name)

    def create(self):
        out_path, out_name = os.path.split(self.id)
        arcpy.management.CreateTable(out_path, out_name)

class TableView(ArcPyEntityView, TableLikeArcPyEntityBase):
    def create(self, source, name):
        arcpy.management.MakeTableView(source.id, name)

