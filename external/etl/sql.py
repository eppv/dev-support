import os
from os.path import split
from witness import Batch
from witness.providers.pandas.loaders import PandasSQLLoader
from external.utils.var import color
from external.native.connections import get_engine
from external.utils.sql.get import get_by_period


def extract(config: dict):
    src_config = config['extract']['src']
    params = src_config['params']
    engine = get_engine(src_config['conn_id'])
    table = f"{src_config['schema']}.{src_config['table']}"
    period_col = params['period_col']
    start = params['start']
    end = params['end']
    extracted_data = get_by_period(
        engine=engine,
        table=table,
        period_col=period_col,
        start=start,
        end=end
    )

    return extracted_data


def load_clean(meta, engine, config):

    schema = config['schema']
    table = config['table']
    meta_elements = config['meta_elements']

    batch = Batch(meta=meta)
    batch.restore()

    loader = PandasSQLLoader(engine=engine, schema=schema, table=table)

    batch.push(loader, meta_elements=meta_elements)
    filename = split(batch.meta['record_source'])[1]
    os.remove(batch.meta['dump_uri'])

    print(f'Batch from {color(filename, "yellow")} loaded, dump cleaned.')
    print(batch.info())
