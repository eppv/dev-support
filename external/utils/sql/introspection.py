import sqlalchemy.exc

from external.utils.sql.db import sql_execute
from external.utils.var import color


def get_columns(engine, schema, table, **kwargs):
    query = f"""
    select column_name, data_type
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_SCHEMA = \'{schema}\' 
      and TABLE_NAME = \'{table}\';
    """
    result = sql_execute(engine, query=query).fetchall()
    return result


def get_loaded_src_ids(engine, table, **kwargs):
    query = f'select distinct record_source from {table};'

    try:
        src_list = sql_execute(engine, query)
        names = [name for name, in src_list]
        return names
    except sqlalchemy.exc.ProgrammingError:
        print(f'Table "{color(table, "yellow")}" does not exist! Filenames list is empty!')
        return []
