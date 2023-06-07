import datetime
import json
import os
import sys
import warnings # предупреждения
import pandas as pd
import pendulum
from autots import AutoTS
from witness import Batch
from witness.providers.pandas.core import PandasExtractor

from external.utils.fs import render_dump_path

warnings.simplefilter("ignore") # убирает предупреждения


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return super().default(o)


def get_data(uri):
    data = pd.read_pickle(uri)
    df = pd.DataFrame(data)
    print('Got df from extracted and dumped data:')
    print(df)
    return df


def get_forecast_daterange(config):
    pred_config = config['transform']['predict']
    forecast_depth = pred_config['forecast_depth']

    end = pendulum.now().date()
    start = (end - pd.DateOffset(forecast_depth)).date()

    date_range = pd.date_range(start, end)
    return date_range

def get_forecast_df_range(df, config):
    forecast_daterange = get_forecast_daterange(config)

    forecast_df_range = []

    for forecast_date in forecast_daterange:
        forecast_start_date = forecast_date - pd.DateOffset(config['transform']['predict']['retrospective_data_depth'])

        print(f'Forecast start: {forecast_start_date}, forecast end: {forecast_date}')

        forecast_df = df[(df['date'] >= forecast_start_date.date()) & (df['date'] < forecast_date.date())]
        forecast_df_range.append(forecast_df)

    return forecast_df_range


def predict(df, config):

    long = False

    pred_config = config['transform']['predict']
    model_list = pred_config.setdefault('model_list', ['DatepartRegression'])
    forecast_depth = pred_config.setdefault('forecast_depth', 14)
    num_validations = pred_config.setdefault('num_validations', 1)
    max_generations = pred_config.setdefault('max_generations', 15)
    drop_most_recent = pred_config.setdefault('drop_most_recent', 1)

    df = df.set_index('date')
    df.index = df.index.astype('datetime64[ns]')

    model = AutoTS(
        forecast_length=forecast_depth,
        frequency='infer',
        prediction_interval=0.9,
        ensemble=None,
        model_list=model_list,  # "superfast", "default", "fast_parallel"
        transformer_list="fast",  # "superfast",
        drop_most_recent=drop_most_recent,
        max_generations=max_generations,
        num_validations=num_validations,
        validation_method="backwards"
    )

    model = model.fit(
        df,
        date_col='datetime' if long else None,
        value_col='value' if long else None,
        id_col='series_id' if long else None,
    )

    prediction = model.predict()

    print(model)

    combined_forecast = pd.concat([prediction.forecast.rename(columns={'cnt_quantity': 'target'}),
               prediction.upper_forecast.rename(columns={'cnt_quantity': 'upper'}),
               prediction.lower_forecast.rename(columns={'cnt_quantity': 'lower'})], axis=1)

    combined_forecast.reset_index(inplace=True)
    result = combined_forecast.rename(columns={'index': 'date'})

    return result



def run_model(config):

    uri = os.getenv('DUMP_PATH')
    cwd_content = os.listdir(os.getcwd())
    print(f'CWD contents: {cwd_content}')
    print(f'Dump file path: {uri}')
    big_df = get_data(uri)

    data = predict(df=big_df, config=config)
    return data


def save_result(result, config):

    src_config = config['extract']['src']
    conn_id = src_config['conn_id']
    schema = src_config['schema']
    table = src_config['table']

    extractor = PandasExtractor(uri=f'{conn_id}:{schema}.{table}')
    extractor.output = result
    print('Setting extraction timestamp...')
    extractor._set_extraction_timestamp() # это тут до обновления witness до >=0.0.6
    batch = Batch()
    print('Filling batch...')
    batch.fill(extractor)
    dump_path = render_dump_path(config, extraction_timestamp=batch.meta['extraction_timestamp'])
    batch.dump(dump_path)

    meta_json_str = json.dumps(batch.meta.__dict__, cls=DateTimeEncoder)
    sys.stdout.write(meta_json_str)
    return batch.meta
