import arcpy
import ec
import textwrap


def field_exist(fc, name):
    for field in arcpy.ListFields(fc):
        if field.name == name:
            return True
    return False

def create_field(fc, field_name, field_type):
    if field_exist(fc, field_name):
        arcpy.management.DeleteField(fc, field_name)
    arcpy.management.AddField(fc, field_name, field_type)

def add_rank_field(model, field_name):

    create_field(model.axis_segment_fc, field_name, 'LONG')

    code_block = textwrap.dedent("""\
    def calculate_rang(k, n):
        if k is None: return -1
        elif n is None: return 2 * k - 1
        else: return 2 * k
    """)


    fl = arcpy.management.MakeFeatureLayer(model.axis_segment_fc, 'axes')
    try:
        arcpy.management.AddJoin(fl, 'LSA', model.node_lsa_fc, 'LSA')
        arcpy.management.AddJoin(fl, 'N_Einfluss', model.node_influence_fc, 'N_Einfluss')

        arcpy.management.CalculateField(in_table=fl, field=field_name,
            expression='calculate_rang(!K_LSA.K_Rang!, !N_Einflussbereich.N_Rang!)',
            expression_type='PYTHON_9.3', code_block=code_block)
    finally:
        arcpy.management.Delete(fl)


def add_id_field(model, field_name):
    create_field(model.axis_segment_fc, field_name, 'LONG')

    code_block = textwrap.dedent("""\
    id = 0
    def autoIncrement():
        global id
        id += 1
        return id
    """)

    arcpy.management.CalculateField(in_table=model.axis_segment_fc,
        field=field_name, expression='autoIncrement()',
        expression_type='PYTHON_9.3', code_block=code_block)

if __name__ == '__main__':
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = r'C:\tsc\workspace'
    model = ec.AxisModel.for_dir(r'C:\tsc\model')

    add_id_field(model, 'segment_id')
    add_rank_field(model, 'S_Rang')
