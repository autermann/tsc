import textwrap
from config import axis_model as model, setenv, fgdb
from arcpy import Polyline, Array, PointGeometry
from utils import SQL

SEGMENT_BUFFER_SIZE = 20

def add_segment_bbox_area():
    model.segments.add_field('bbox_area', 'DOUBLE')

    buffer_distance = '{0} Meters'.format(str(SEGMENT_BUFFER_SIZE))
    segments_buffered = fgdb.feature_class('segments_buffered')
    segments_bbox = fgdb.feature_class('segments_bbox')
    segments = model.segments.view()
    try:
        segments_buffered.delete_if_exists()
        segments_bbox.delete_if_exists()

        segments.buffer(segments_buffered, buffer_distance)
        segments_buffered.minimum_bounding_geometry(segments_bbox)

        segments_bbox.add_field('area', 'DOUBLE')
        segments_bbox.calculate_field('area', '!shape.geodesicArea@SQUAREKILOMETERS!')

        segments.add_join('segment_id', segments_bbox, 'segment_id')


        segments.calculate_field('bbox_area', '!segments_bbox.area!')
    finally:
        segments_buffered.delete_if_exists()
        segments.delete_if_exists()
        segments_bbox.delete_if_exists()


def add_segment_rank():
    model.segments.add_field('rank', 'LONG')

    with model.influence_nodes.search(['Achsen_ID', 'N_Rang', 'N_Einfluss']) as sc:
        for axis, rank, name in sc:
            lsa = 'K' + name[1:-1]
            rank = 2 * (rank - 1)
            where_clause = SQL.and_((
                    SQL.eq_('Achsen_ID', SQL.quote_(axis)),
                    SQL.eq_('LSA', SQL.quote_(lsa)),
                    SQL.eq_('Segmenttyp', 0)))
            with model.segments.update(['Segmenttyp', 'rank'], where_clause=where_clause) as rows:
                for row in rows:
                    row[1] = rank
                    rows.updateRow(row)

    with model.lsa_nodes.search(['Achsen_ID', 'K_Rang', 'LSA']) as sc:
        for axis, rank, name in sc:
            lsa = name[:-1]
            rank = 2 * (rank - 1) + 1
            where_clause = SQL.and_((
                    SQL.eq_('Achsen_ID', SQL.quote_(axis)),
                    SQL.eq_('LSA', SQL.quote_(lsa)),
                    SQL.eq_('Segmenttyp', 1)))
            with model.segments.update(['rank'], where_clause=where_clause) as rows:
                for row in rows:
                    row[0] = rank
                    rows.updateRow(row)

def add_length_and_duration():
    model.segments.add_field('length', 'DOUBLE')
    model.segments.add_field('duration', 'DOUBLE')

    with model.segments.update(['SHAPE@', 'length', 'duration' ]) as rows:
        for row in rows:
            shape = row[0]
            array = Array([shape.firstPoint, shape.lastPoint])
            polyline = Polyline(array, shape.spatialReference)
            row[1] = shape.getLength('GEODESIC', 'METERS')
            row[2] = row[1] / (50/3.6)
            rows.updateRow(row)


if __name__ == '__main__':
    setenv()
    fgdb.create_if_not_exists()
    add_segment_bbox_area()
    add_length_and_duration()
    add_segment_rank()


