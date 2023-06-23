import os

from witness import Batch
from witness.providers.pandas.extractors import PandasExcelExtractor
from witness.providers.pandas.loaders import PandasExcelLoader
from external.utils.var import color
from external.etl.transform.scenario import Scenario
from external.utils.configparse import get_trf_config_and_sequence


def _transform(df, config):
    try:
        trf_config, trf_seq = get_trf_config_and_sequence(config)
        scenario = Scenario(sequence=trf_seq, config=trf_config)
        transformed_df = scenario.apply(df)

        return transformed_df
    except KeyError:
        print('No transform function found.')
        return df



def _handle_multi_sheet(output, config):
    dfs = []
    for sheet_name, df in output.items():
        df.dropna(axis=1, inplace=True, how='all')
        transformed_df = _transform(df, config)
        transformed_df['sheet_name'] = sheet_name
        dfs.append(transformed_df)
    return dfs


def _handle_single_sheet(output, config):
    output.dropna(axis=1, inplace=True, how='all')
    transformed_df = _transform(output, config)
    return [transformed_df]


def extract_and_normalize(extractor, config):
    from pandas import concat
    extractor.extract()
    raw_output = extractor.output

    dfs = _handle_multi_sheet(raw_output, config) if extractor.sheet_name is None \
        else _handle_single_sheet(raw_output, config)

    united_df = concat(dfs)
    setattr(extractor, 'output', united_df)

    print(extractor.output)

    extractor.unify()

    return extractor.output


def extract(config):

    src_cfg = config['extract']['src']
    uri = src_cfg['uri']
    dump_dir = config['dump']

    if not is_excel_format(uri):
        return None

    extractor = PandasExcelExtractor(**src_cfg, dtype='string',)
    print(f'Extracting from {uri}')
    output = extract_and_normalize(extractor, config=config)

    batch = Batch(data=output['data'], meta=output['meta'])

    filename = os.path.split(uri)[1]
    extraction_ts_str = batch.meta['extraction_timestamp'].strftime('%Y-%m-%d_%H-%M-%S_%f')
    dump_path = f"{dump_dir}/dump_{filename}_{extraction_ts_str}"
    batch.dump(dump_path)
    print(f'+ Batch from {color(filename, "yellow")} extracted and persisted.')
    print(batch.info())

    return batch.meta


def is_excel_format(filepath):
    excel_extensions = ['.xlsx', '.xls', '.xlsm', '.xlsb']
    head, tail = os.path.split(filepath)
    name, ext = os.path.splitext(tail)
    if str.lower(ext) in excel_extensions:
        return True
    print(f'{filepath} is not excel format. Passing...')
    return False


def is_valid(filepath):
    tempfile_substrings = ['.tmp', '~$']
    if os.path.isfile(filepath):
        if any(substring in filepath for substring in tempfile_substrings):
            print(f'{filepath} is temp file. Passing...')
            return False
        return True

    else:
        print(f'{filepath} is not a file. Passing...')
        return False


def load_clean(meta, config):
    sink = config['load']['sink']
    uri = sink['uri']
    sheet_name = sink.setdefault('sheet_name', 'Sheet1')
    meta_elements = sink['meta_elements']

    batch = Batch(meta=meta)
    batch.restore()

    loader = PandasExcelLoader(uri=uri, sheet_name=sheet_name)
    batch.push(loader, meta_elements=meta_elements)

    filename = os.path.split(batch.meta['record_source'])[1]
    os.remove(batch.meta['dump_uri'])

    print(f'Batch from {color(filename, "yellow")} loaded, dump cleaned.')
    print(batch.info())