from datetime import datetime
from witness import Batch
from external.utils.var import show_exec_time, color


def _drop_empty_columns_in_output(extractor):
    extractor.output.dropna(axis=1, inplace=True, how='all')
    return extractor.unify().output


@show_exec_time
def extract(extractor):
    print(f'Starting at {str(datetime.now())}')
    print(f'Extracting from {color(extractor.uri, "yellow")}...')
    extr_output = _drop_empty_columns_in_output(extractor.extract())
    batch = Batch(data=extr_output['data'], meta=extr_output['meta'])
    print('Extracted.')
    return batch


def create_backup(batch, path):
    print(f'Creating dump file at: {path}')
    batch.dump(path)
    print(f'Dump file created.')


@show_exec_time
def load(batch, loader, meta_elements: list):
    print(f'Starting load at {str(datetime.now())}')
    batch.push(loader, meta_elements=meta_elements)
    print(f'Loaded to {color(loader.uri, "yellow")}')


@show_exec_time
def transform_mct_plans_and_rejects(output):

    def _remove_totals(dataframe):
        matching_condition = dataframe["Пункт перевалки"].str.contains("итог", case=False) \
                             | dataframe["Наим. группы груза"].str.contains("итог", case=False)
        dataframe.drop(dataframe[matching_condition].index, inplace=True)
        return dataframe

    def _ffilna(dataframe):
        dataframe.fillna(method='ffill', inplace=True)
        return dataframe

    result = _ffilna(_remove_totals(output))

    return result
