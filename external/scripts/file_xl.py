
from witness.providers.pandas.extractors import PandasExcelExtractor
from witness.providers.pandas.loaders import PandasSQLLoader
from sqlalchemy import create_engine

from external.etl.common import extract, create_backup, load, \
    get_etl_config
from external.utils.db import apply_on_db, \
    dummy_db_action
from external.utils.var import show_exec_time
from external.utils.connections import get_connection


script_id = 'example'

CONFIG = get_etl_config('../config/example')[script_id]
DSN = get_connection(CONFIG['load']['sink']['conn_id']).render_db_dsn()


@show_exec_time
def run(config):

    uri = config['extract']['src']['uri']
    header = config['extract']['src']['header']

    schema = config['load']['sink']['schema']
    table = config['load']['sink']['table']

    dump_path = config['dump']

    sql_engine = create_engine(DSN, echo=False)
    file_extractor = PandasExcelExtractor(uri, dtype='string', header=header)
    db_loader = PandasSQLLoader(engine=sql_engine, schema=schema, table=table)

    batch = extract(file_extractor)

    create_backup(batch, dump_path)
    preparation_actions = [dummy_db_action]
    apply_on_db(engine=sql_engine, table=f'{schema}.{table}', actions=preparation_actions)

    meta_elements_to_attach = ['extraction_timestamp', 'record_source']
    load(batch, db_loader, meta_elements_to_attach)


if __name__ == '__main__':
    run(CONFIG)
