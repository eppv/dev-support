
from external.native.connections import Connection
from external.utils.var import render_dump_path


def get_extract(config: dict):
    from witness import Batch
    from witness.extractors.http import JsonHttpGetExtractor

    src_config = config['extract']['src']

    src_conn = Connection.from_config(config['extract']['src']['conn_id'])
    uri = f'{src_conn.host}{src_config["method"]}'
    params = src_config['params']
    extractor = JsonHttpGetExtractor(uri=uri, params=params)
    batch = Batch()
    batch.fill(extractor)

    dump_path = render_dump_path(config, extractor.extraction_timestamp)
    batch.dump(dump_path)

    return batch.meta
