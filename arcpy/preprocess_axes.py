import textwrap
from config import axis_model, setenv

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

if __name__ == '__main__':
    setenv()
    axis_model.segments.add_id_field('segment_id')
    add_rank_field(axis_model, 'S_Rang')
