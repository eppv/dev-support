import os
from os import listdir
from os.path import isfile, split
from sqlalchemy import create_engine
from witness.providers.pandas.extractors import PandasExcelExtractor
from witness.providers.pandas.loaders import PandasSQLLoader

from external.utils.var import show_exec_time
from external.etl.common import extract, create_backup, load, get_etl_config
from external.utils.db import get_loaded_src_ids, grant_preset_priveleges, \
    apply_on_db, dummy_db_action
from external.utils.connections import get_connection


def is_temp(filename):
    if '~$' in filename:
        return True
    return False


@show_exec_time
def file_etl(filepath, engine, config):

    uri = filepath
    schema = config['schema']
    table = config['table']
    header = config['header']
    head, tail = split(uri)
    dump_path = f"{config['dump']}_{tail}"

    file_extractor = PandasExcelExtractor(uri, dtype='string', header=header)
    db_loader = PandasSQLLoader(engine=engine, schema=schema, table=table)

    batch = extract(file_extractor)
    create_backup(batch, dump_path)

    meta_elements_to_attach = ['extraction_timestamp', 'record_source']
    print(batch.meta['dump_uri'])

    load(batch, db_loader, meta_elements_to_attach)
    os.remove(dump_path)
    print(f'{tail} dump removed.')


@show_exec_time
def dir_etl(config):

    src_folder = config['uri']
    schema = config['schema']
    table = config['table']
    sql_engine = create_engine(DSN, echo=False, pool_pre_ping=True)

    preparation_actions = [dummy_db_action]
    apply_on_db(engine=sql_engine, table=f'{schema}.{table}', actions=preparation_actions)

    filenames = listdir(src_folder)
    loaded_files = get_loaded_src_ids(sql_engine, f'{schema}.{table}')

    for file_name in filenames:
        file_path = rf'{src_folder}\{file_name}'
        if isfile(file_path):
            if is_temp(file_name):
                print(f'{file_name} is temp file. Passing...')
                continue
            if file_path in loaded_files:
                print(f'{file_name} is already loaded. Passing...')
                continue
            else:
                file_etl(file_path, sql_engine, config)
        else:
            print(f'{file_name} is not a file. Passing...')

    final_db_actions = [grant_preset_priveleges]
    apply_on_db(engine=sql_engine, table=f'{schema}.{table}', actions=final_db_actions)


if __name__ == '__main__':

    dag_id = 'iac_forms_cnt_working_area_comments'
    config_path = '../config/iac_forms'
    CONFIG = get_etl_config(config_path)[dag_id]

    src_uri = CONFIG['extract']['src']['uri']
    conn_id = CONFIG['load']['sink']['conn_id']
    dump = CONFIG['dump']

    connection = get_connection(conn_id)

    print(f"""
    src_uri = {src_uri}
    sink_connection_dsn = {connection.render_db_dsn()}
    dump = {dump}
    """)
