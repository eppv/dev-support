import json
import os
from witness.extractors.file import FileExtractor
from witness import Batch
from external.utils.var import render_dump_path, color


class JSONFileExtractor(FileExtractor):

    def extract(self):
        self._set_extraction_timestamp()

        with open(self.uri, 'r', encoding='utf-8') as file:
            data = json.load(file)
            setattr(self, 'output', data)

        return self

    def unify(self):
        data = self.output
        meta = {'extraction_timestamp': self.extraction_timestamp,
                'record_source': self.uri}

        setattr(self, 'output', {'meta': meta, 'data': data})

        return self


def is_json_format(filepath):
    if filepath.endswith('json'):
        return True
    print(f'{filepath} is not json file format. Passing...')
    return False


def extract(config):

    src_cfg = config['extract']['src']
    uri = src_cfg['uri']
    filename = os.path.split(uri)[1]

    if not is_json_format(uri):
        return None

    try:
        tsf_config = config['transform']
    except KeyError:
        tsf_config = None

    extractor = JSONFileExtractor(**src_cfg,)
    print(f'Extracting from {uri}')
    batch = Batch().fill(extractor)

    extraction_ts = batch.meta['extraction_timestamp']
    dump_path = render_dump_path(config, extraction_ts)
    batch.dump(dump_path)
    print(f'+ Batch from {color(filename, "yellow")} extracted and persisted.')
    print(batch.info())

    return batch.meta
