import os
import re
from pathlib import Path
from external.native.connections import get_engine
from external.etl.file.excel import extract, is_valid
from external.etl.sql import load_clean
from external.utils import sql, fs
from external.utils.sql.introspection import get_loaded_src_ids
from external.utils.var import color


def get_filepaths(dirpath):
    dirpath = Path(dirpath)
    if not dirpath.exists():
        print(f"The provided path: {dirpath} does not exist.")
        return None
    all_files_roots = [str(file) for file in dirpath.rglob('*')]
    return all_files_roots


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
    loaded = get_loaded_src_ids(engine, table)
    missing = [path for path in sources if path not in loaded]

    return missing


def filename_contains(filename:str, substrings:list):
    for substring in substrings:
        if substring in filename:
            print(f"The file: {filename} contains inappropriate substring. Passing...")
            return True
    return False


def dir_extract(config):

    src = config['extract']['src']
    sink = config['load']['sink']
    dir_uri = src['uri']

    conn_id = config['load']['sink']['conn_id']
    full_table_name = f"{sink['schema']}.{sink['table']}"
    mode = config['load']['mode']

    filepaths = fs.get_filepaths(dir_uri)
    sources = [f'{filepath}' for filepath in filepaths if is_valid(f'{filepath}')]

    if 'filter' in config['extract']:
        filter_condition = config['extract']['filter']
        sources = [str(file) for file in sources]
        sources = [Path(file) for file in sources if re.search(filter_condition, file)]
    else:
        pass


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
    from external.utils.sql.common import prog_exc

    sink = config['load']['sink']
    conn_id = sink['conn_id']
    schema = sink['schema']
    table = sink['table']
    full_table_name = f'{schema}.{table}'
    mode = config['load']['mode']

    engine = get_engine(conn_id)

    db_actions_map = {
        'incremental': [sql.dummy_db_action],
        'replace': [sql.delete.truncate_table]
    }

    try:
        db_on_start = db_actions_map[mode]
    except KeyError:
        db_on_start = []
        print(f'{color("WARNING:", "red")} Unknown load mode. Operation will be canceled.')
    db_on_end = [sql.grant.preset_priveleges]

    try:
        sql.apply_on_db(engine=engine, table=full_table_name, actions=db_on_start)
    except sql.prog_exc:
        print(f'Table {color(table), "yellow"} does not exist and will be created by loader.')

    for batch_meta in meta:
        if batch_meta is not None:
            load_clean(meta=batch_meta, config=config)

    sql.apply_on_db(engine=engine, table=full_table_name, actions=db_on_end)

