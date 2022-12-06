import os
from os.path import split
from witness import Batch
from witness.providers.pandas.loaders import PandasSQLLoader
from external.utils.var import color


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
