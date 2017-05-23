import textwrap
import config

SEGMENT_BUFFER_SIZE = 20

def add_segment_bbox_area():
    config.axis_model.segments.add_field('bbox_area', 'DOUBLE')

    buffer_distance = '{0} Meters'.format(str(SEGMENT_BUFFER_SIZE))
    segments_buffered = config.fgdb.feature_class('segments_buffered')
    segments_bbox = config.fgdb.feature_class('segments_bbox')
    segments = config.axis_model.segments.view()
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
    config.axis_model.segments.add_field('rank', 'LONG')

    with config.axis_model.segments.update(['Segmenttyp', 'Rang_LSA', 'rank']) as rows:
        for row in rows:
            # non-influence
            if row[0] == 0:
                row[2] = 2 * (row[1] - 1)
            # influence
            elif row[0] == 1:
                row[2] = 2 * (row[1] - 1) + 1
            rows.updateRow(row)

def add_length_and_duration():
    config.axis_model.segments.add_field('length', 'DOUBLE')
    config.axis_model.segments.calculate_field('length', '!laenge!')
    config.axis_model.segments.add_field('duration', 'DOUBLE')
    config.axis_model.segments.calculate_field('duration', '!laenge!/(50/3.6)')

if __name__ == '__main__':
    config.setenv()
    config.fgdb.create_if_not_exists()
    add_segment_bbox_area()
    add_length_and_duration()
    add_segment_rank()
