import os
import re
from pathlib import Path
from external.etl.file.excel import extract, is_valid
from external.utils import fs, sql


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
    dir_uri = src['uri']
    filepaths = fs.get_filepaths(dir_uri)
    sources = [f'{filepath}' for filepath in filepaths if is_valid(f'{filepath}')]

    rx_condition = config['extract'].setdefault('filter', None)
    if rx_condition is not None:
        sources = [Path(file) for file in sources if re.search(rx_condition, str(file))]

    files_to_extract = sql.introspection.filter_loaded_sources(sources, config)

    extracted = []

    for uri in files_to_extract:
        file_config = config.copy()
        file_config['extract']['src']['uri'] = uri
        meta = extract(config=config)
        extracted.append(meta)

    return extracted
