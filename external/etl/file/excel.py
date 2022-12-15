from os.path import split

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


def _extract_and_normalize(extractor, config, transformation=None):
    from pandas import concat
    extractor.extract()
    raw_output = extractor.output

    dfs = _handle_multi_sheet(raw_output, config, transformation) if extractor.sheet_name is None \
        else _handle_single_sheet(raw_output, config, transformation)

    united_df = concat(dfs)
    setattr(extractor, 'output', united_df)

    extractor.unify()

    return extractor.output


def extract(uri, config, dump_dir=None, transformation=None):

    src_cfg = config['extract']['src']
    if transformation is not None:
        tsf_config = config['transform']
    else:
        tsf_config = None
    src_cfg['uri'] = uri

    extractor = PandasExcelExtractor(**src_cfg, dtype='string',)
    print(f'Extracting from {uri}')
    output = _extract_and_normalize(extractor, config=tsf_config, transformation=transformation)

    batch = Batch(data=output['data'], meta=output['meta'])

    filename = split(uri)[1]
    extraction_ts_str = batch.meta['extraction_timestamp'].strftime('%Y-%m-%d_%H-%M-%S_%f')
    dump_path = f"{dump_dir}/dump_{filename}_{extraction_ts_str}"
    batch.dump(dump_path)
    print(f'+ Batch from {color(filename, "yellow")} extracted and persisted.')
    print(batch.info())

    return batch.meta

