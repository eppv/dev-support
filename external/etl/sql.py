import os
import pandas as pd
import pendulum

from witness import Batch
from witness.providers.pandas.loaders import PandasSQLLoader
from witness.providers.pandas.extractors import PandasExtractor

from external import LOCAL_TZ_NAME
from external.utils.var import color
from external.native.connections import get_engine
from external.utils.sql.get import get_by_period, get_all, get_by_custom_query
from external.utils.var import render_dump_path

FUNC_MAPPING = {
    'get_all': get_all,
    'get_by_period': get_by_period,
    'get_by_custom_query': get_by_custom_query
}

def extract(config: dict, get_func: callable = get_by_period):
    src_config = config['extract']['src']
    params = src_config.setdefault('params', {})

    engine = get_engine(src_config['conn_id'])
    table = f"{src_config['schema']}.{src_config['table']}"

    extracted_data = get_func(
        engine=engine,
        table=table,
        **params
    )
    extr_ts = pendulum.now(LOCAL_TZ_NAME)
    df = pd.DataFrame(extracted_data)
    extractor = PandasExtractor(uri=f'{engine.url.database}.{table}')
    setattr(extractor, 'output', df)
    setattr(extractor, 'extraction_timestamp', extr_ts)

    output = extractor.unify().output
    batch = Batch(data=output['data'], meta=output['meta'])
    dump_uri = render_dump_path(config, extr_ts)
    batch.dump(uri=dump_uri)

    return batch.meta


def load_clean(meta, config):
    sink = config['load']['sink']
    conn_id = sink['conn_id']
    schema = sink['schema']
    table = sink['table']
    meta_elements = sink['meta_elements']
    engine = get_engine(conn_id)

    batch = Batch(meta=meta)
    batch.restore()

    loader = PandasSQLLoader(engine=engine, schema=schema, table=table)

    batch.push(loader, meta_elements=meta_elements)
    filename = os.path.split(batch.meta['record_source'])[1]
    os.remove(batch.meta['dump_uri'])

    print(f'Batch from {color(filename, "yellow")} loaded, dump cleaned.')
    print(batch.info())

