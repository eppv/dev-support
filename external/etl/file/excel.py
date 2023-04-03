import os

from witness import Batch
from witness.providers.pandas.extractors import PandasExcelExtractor
from external.utils.var import color


def _handle_multi_sheet(output, config, transformation=None):
    dfs = []
    for sheet_name, df in output.items():
        df.dropna(axis=1, inplace=True, how='all')
        if transformation is not None:
            transformed_df = transformation(df, config)
        else:
            transformed_df = df
        transformed_df['sheet_name'] = sheet_name
        dfs.append(transformed_df)
    return dfs


def _handle_single_sheet(output, config, transformation=None):
    output.dropna(axis=1, inplace=True, how='all')
    if transformation is not None:
        transformed_df = transformation(output, config)
    else:
        transformed_df = output
    return [transformed_df]


def extract_and_normalize(extractor, config, transformation=None):
    from pandas import concat
    extractor.extract()
    raw_output = extractor.output

    dfs = _handle_multi_sheet(raw_output, config, transformation) if extractor.sheet_name is None \
        else _handle_single_sheet(raw_output, config, transformation)

    united_df = concat(dfs)
    setattr(extractor, 'output', united_df)

    extractor.unify()

    return extractor.output


def extract(config, transform=None):

    src_cfg = config['extract']['src']
    uri = src_cfg['uri']
    dump_dir = config['dump']

    if not is_excel_format(uri):
        return None

    if transform is not None:
        tsf_config = config['transform']
    else:
        tsf_config = None

    extractor = PandasExcelExtractor(**src_cfg, dtype='string',)
    print(f'Extracting from {uri}')
    output = extract_and_normalize(extractor, config=tsf_config, transformation=transform)

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
    if ext in excel_extensions:
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
