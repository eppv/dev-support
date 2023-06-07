import sqlalchemy.exc

from external.native.connections import get_engine
from external.utils.sql.common import execute
from external.utils.var import color


def get_columns(engine, schema, table, **kwargs):
    query = f"""
    select column_name, data_type
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_SCHEMA = \'{schema}\' 
      and TABLE_NAME = \'{table}\';
    """
    result = execute(engine, query=query)
    return result


def get_loaded_src_ids(engine, table, **kwargs):
    query = f'select distinct record_source from {table};'

    try:
        src_list = execute(engine, query)
        names = [name for name, in src_list]
        return names
    except sqlalchemy.exc.ProgrammingError:
        print(f'Table "{color(table, "yellow")}" does not exist! Filenames list is empty!')
        return []


def check_missing_sources(sources, conn_id, table):
    engine = get_engine(conn_id)
    loaded = get_loaded_src_ids(engine, table)
    missing = [path for path in sources if path not in loaded]

    return missing