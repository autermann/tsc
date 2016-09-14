
from config import fgdb, axis_model as model, setenv
from ec import create_result_tables

if __name__ == '__main__':
    setenv()
    create_result_tables(fgdb, model)