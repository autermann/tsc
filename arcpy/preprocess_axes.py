import textwrap
from config import axis_model, setenv
from arcpy import Polyline, Array, PointGeometry

def add_rank_field(model, field_name):
    if self.field_exist(field_name):
        self.delete_field(field_name)
    self.add_field(field_name, 'LONG')



    code_block = textwrap.dedent("""\
    def calculate_rang(k, n):
        if k is None: return -1
        elif n is None: return 2 * k - 1
        else: return 2 * k
    """)


    fl = model.segments.view('axes')
    try:
        fl.join('LSA', model.lsa_nodes, 'LSA')
        fl.join('N_Einfluss', model.influence_nodes, 'N_Einfluss')
        expression = 'calculate_rang(!K_LSA.K_Rang!, !N_Einflussbereich.N_Rang!)'
        fl.calculate_field(field_name, expression, code_block=code_block)
    finally:
        fl.delete()


def add_length_fields(model):
    fc = model.segments
    fc.add_field('length', 'DOUBLE')
    fc.add_field('blength', 'DOUBLE')
    fc.add_field('dlength', 'DOUBLE')
    fc.add_field('duration', 'DOUBLE')
    fc.add_field('bduration', 'DOUBLE')
    fc.add_field('dduration', 'DOUBLE')


    with fc.update(['SHAPE@', 'length', 'blength', 'dlength', 'duration', 'bduration', 'dduration']) as rows:
        for row in rows:
            shape = row[0]
            array = Array([shape.firstPoint, shape.lastPoint])
            polyline = Polyline(array, shape.spatialReference)
            row[1] = shape.getLength('GEODESIC', 'METERS')
            row[2] = polyline.getLength('GEODESIC', 'METERS')
            row[3] = abs(row[1]-row[2])
            row[4] = row[1] / (50/3.6)
            row[5] = row[2] / (50/3.6)
            row[6] = abs(row[4]-row[5])

            rows.updateRow(row)

if __name__ == '__main__':
    setenv()
    #axis_model.segments.add_id_field('segment_id')
    #add_rank_field(axis_model, 'S_Rang')
    add_length_fields(axis_model)
