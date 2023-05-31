import os
from external.native.connections import get_engine
from external.etl.file.excel import extract, is_valid
from external.etl.sql import load_clean
from external.utils import sql, fs
from external.utils.sql.common import dummy_db_action, apply_on_db
from external.utils.sql.delete import truncate_table
from external.utils.sql.grant import grant_preset_priveleges
from external.utils.sql.introspection import check_missing_sources
from external.utils.var import color


def print_etl_debug_msg(config):

    src = config['extract']['src']
    sink = config['load']['sink']

    uri = src['uri']

    schema = sink['schema']
    table = sink['table']
    meta_elements_to_attach = sink['meta_elements']
    mode = config['load']['mode']

    tail = os.path.split(uri)[1]
    dump_path = f"{config['dump']}_{tail}"

    debug_msg = f'''
        From: {uri} 
        Dump creating at: {dump_path}
        Loading to: {schema}.{table}
        Load mode: {mode}
        Attaching meta: {meta_elements_to_attach}
        '''

    print(debug_msg)


def dir_extract(config):

    src = config['extract']['src']
    sink = config['load']['sink']
    dir_uri = src['uri']

    conn_id = config['load']['sink']['conn_id']
    full_table_name = f"{sink['schema']}.{sink['table']}"
    mode = config['load']['mode']

    filepaths = fs.get_filepaths(dir_uri)
    sources = [f'{filepath}' for filepath in filepaths if is_valid(f'{filepath}')]

    if mode == 'incremental':
        files_to_extract = sql.introspection.check_missing_sources(sources, conn_id, full_table_name)
    elif mode == 'replace':
        files_to_extract = sources
    else:
        files_to_extract = []
        print(f'{color("WARNING:", "red")} Unknown load mode. Operation will be canceled.')

    extracted = []

    for uri in files_to_extract:
        file_config = config.copy()
        file_config['extract']['src']['uri'] = uri
        meta = extract(config=config)
        extracted.append(meta)

    return extracted


def dir_load(meta, config):

    sink = config['load']['sink']
    conn_id = sink['conn_id']
    schema = sink['schema']
    table = sink['table']
    full_table_name = f'{schema}.{table}'
    mode = config['load']['mode']

    engine = get_engine(conn_id)

    db_actions_map = {
        'incremental': [sql.common.dummy_db_action],
        'replace': [sql.delete.truncate_table]
    }

    try:
        db_on_start = db_actions_map[mode]
    except KeyError:
        db_on_start = []
        print(f'{color("WARNING:", "red")} Unknown load mode. Operation will be canceled.')
    db_on_end = [sql.grant.grant_preset_priveleges]

    try:
        sql.apply_on_db(engine=engine, table=full_table_name, actions=db_on_start)
    except sql.common.sql_prog_exc:
        print(f'Table {color(table), "yellow"} does not exist and will be created by loader.')

    for batch_meta in meta:
        if batch_meta is not None:
            load_clean(meta=batch_meta, config=config)

    sql.apply_on_db(engine=engine, table=full_table_name, actions=db_on_end)

