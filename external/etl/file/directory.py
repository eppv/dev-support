import os

from sqlalchemy import create_engine
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from external.native.db import get_engine

from external.etl.file.excel import extract, is_valid
from external.etl.sql import load_clean
from external.utils.db import dummy_db_action, grant_preset_priveleges, get_loaded_src_ids, \
    truncate_table, apply_on_db
from external.utils.var import color


def get_filepaths(dirpath):
    files_roots = []
    for root, dirs, filenames in os.walk(dirpath):
        for filename in filenames:
            files_roots.append(os.path.join(root, filename))

    return files_roots


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


def check_missing_sources(sources, conn_id, table):
    engine = get_engine(conn_id)
    # engine = PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine()
    loaded = get_loaded_src_ids(engine, table)
    missing = [path for path in sources if path not in loaded]

    return missing


def dir_extract(config, custom_transformation=None):

    src = config['extract']['src']
    sink = config['load']['sink']
    dir_uri = src['uri']

    conn_id = config['load']['sink']['conn_id']
    full_table_name = f"{sink['schema']}.{sink['table']}"
    mode = config['load']['mode']

    filepaths = get_filepaths(dir_uri)
    sources = [f'{filepath}' for filepath in filepaths if is_valid(f'{filepath}')]

    if mode == 'incremental':
        files_to_extract = check_missing_sources(sources, conn_id, full_table_name)
    elif mode == 'replace':
        files_to_extract = sources
    else:
        files_to_extract = []
        print(f'{color("WARNING:", "red")} Unknown load mode. Operation will be canceled.')

    extracted = []

    for uri in files_to_extract:
        file_config = config.copy()
        file_config['extract']['src']['uri'] = uri
        meta = extract(config=config, transformation=custom_transformation)
        extracted.append(meta)

    return extracted


def dir_load(meta, config):
    from external.utils.db import sql_prog_exc

    sink = config['load']['sink']
    conn_id = sink['conn_id']
    schema = sink['schema']
    table = sink['table']
    full_table_name = f'{schema}.{table}'
    mode = config['load']['mode']

    engine = get_engine(conn_id)

    db_actions_map = {
        'incremental': [dummy_db_action],
        'replace': [truncate_table]
    }

    try:
        db_on_start = db_actions_map[mode]
    except KeyError:
        db_on_start = []
        print(f'{color("WARNING:", "red")} Unknown load mode. Operation will be canceled.')
    db_on_end = [grant_preset_priveleges]

    try:
        apply_on_db(engine=engine, table=full_table_name, actions=db_on_start)
    except sql_prog_exc:
        print(f'Table {color(table), "yellow"} does not exist and will be created by loader.')

    for batch_meta in meta:
        if batch_meta is not None:
            load_clean(batch_meta, engine, config=sink)

    apply_on_db(engine=engine, table=full_table_name, actions=db_on_end)

