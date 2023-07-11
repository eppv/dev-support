
from external.utils.sql.common import sql_execute, prog_exc
from external.utils.var import color
from external.native.connections import get_engine


def get_columns(engine, schema, table, **kwargs):
    query = f"""
    select column_name, data_type
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_SCHEMA = \'{schema}\' 
      and TABLE_NAME = \'{table}\';
    """
    result = sql_execute(engine, query=query)
    return result


def get_loaded_src_ids(engine, table, **kwargs):
    query = f'select distinct record_source from {table};'

    try:
        src_list = sql_execute(engine, query)
        names = [name for name, in src_list]
        return names
    except prog_exc as exc:
        print(f'Table "{color(table, "yellow")}" does not exist! Filenames list is empty!')
        raise exc


def select_missing_sources(sources: list, conn_id: str, table: str) -> list:

    engine = get_engine(conn_id)
    try:
        loaded = get_loaded_src_ids(engine, table)
    except prog_exc:
        print('There are no loaded sources to check because of the sink table doesnt exist.')
        loaded = []
    missing = [path for path in sources if path not in loaded]

    return missing


def filter_loaded_sources(uris: list, config: dict) -> list:
    sink = config['load']['sink']
    conn_id = sink['conn_id']
    full_table_name = f"{sink['schema']}.{sink['table']}"
    mode = config['load']['mode']

    if mode == 'incremental':
        sources = select_missing_sources(uris, conn_id, full_table_name)
    elif mode == 'replace':
        sources = uris
    else:
        sources = []
        print(f'{color("WARNING:", "red")} Unknown load mode. Operation will be canceled.')

    return sources
