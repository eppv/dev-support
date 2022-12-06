from os import walk
from os.path import split, isfile

from sqlalchemy import create_engine
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from external.utils.connections import Connection

from external.etl.file.excel import extract
from external.etl.sql import load_clean
from external.utils.db import dummy_db_action, grant_preset_priveleges, get_loaded_src_ids, \
    truncate_table, apply_on_db
from external.utils.var import color


def is_valid(filepath):
    if isfile(filepath):
        if '~$' in filepath:
            print(f'{filepath} is temp file. Passing...')
            return False
        return True
    else:
        print(f'{filepath} is not a file. Passing...')
        return False


def print_etl_debug_msg(uri, engine, config):

    src = config['extract']['src']
    sink = config['load']['sink']
    header = src['header']

    schema = sink['schema']
    table = sink['table']
    meta_elements_to_attach = sink['meta_elements']
    mode = config['load']['mode']

    tail = split(uri)[1]
    dump_path = f"{config['dump']}_{tail}"

    debug_msg = f'''
        From: {uri} looking for header on {header} row
        Dump creating at: {dump_path}
        Using: {engine.engine}
        Loading to: {schema}.{table}
        Load mode: {mode}
        Attaching meta: {meta_elements_to_attach}
        '''

    print(debug_msg)


def check_missing_sources(sources, engine, table):

    loaded = get_loaded_src_ids(engine, table)
    missing = [path for path in sources if path not in loaded]

    return missing


def dir_extract(config, custom_transformation=None):

    src = config['extract']['src']
    sink = config['load']['sink']
    dir_uri = src['uri']

    dump_dir = config['dump']

    conn_id = config['load']['sink']['conn_id']
    schema = sink['schema']
    table = sink['table']
    full_table_name = f'{schema}.{table}'
    mode = config['load']['mode']

    # engine = PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine()
    dsn = Connection.from_config(conn_id).render_db_dsn()
    engine = create_engine(dsn)

    all_files = []
    for root, dirs, files in walk(dir_uri):
        for file in files:
            all_files.append(file)
    sources = [f'{dir_uri}/{file}' for file in all_files if is_valid(f'{dir_uri}/{file}')]

    if mode == 'incremental':
        to_extract = check_missing_sources(sources, engine, full_table_name)
    elif mode == 'replace':
        to_extract = sources
    else:
        to_extract = []
        print(f'{color("WARNING:", "red")} Unknown load mode. Operation will be canceled.')

    extracted = []

    for uri in to_extract:
        meta = extract(uri=uri, config=config, dump_dir=dump_dir, transformation=custom_transformation)
        extracted.append(meta)
        # print_etl_debug_msg(uri, engine, config)

    return extracted


def dir_load(meta, config):
    from external.utils.db import sql_prog_exc

    sink = config['load']['sink']
    conn_id = sink['conn_id']
    schema = sink['schema']
    table = sink['table']
    full_table_name = f'{schema}.{table}'
    mode = config['load']['mode']

    # engine = PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine()

    dsn = Connection.from_config(conn_id).render_db_dsn()
    engine = create_engine(dsn)

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
        load_clean(batch_meta, engine, config=sink)

    apply_on_db(engine=engine, table=full_table_name, actions=db_on_end)

