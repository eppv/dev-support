import os
import json
from functions import get_data, predict
from external.utils.configparse import get_config
from external.utils.var import render_dump_path
from external.etl.sql import extract
from witness.providers.pandas.extractors import PandasExtractor
from witness import Batch
from external.utils.sql.get import get_by_custom_query
import datetime

xcom_folder_path = r"D:\data\tmp"


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return super().default(o)


def extract_and_dump(config):

    import pandas as pd
    import pendulum

    src_config = config['extract']['src']
    pred_config = config['transform']['predict']
    schema = src_config['schema']
    table = src_config['table']

    forecast_depth = pred_config['forecast_depth']
    retrospective_data_depth = pred_config['retrospective_data_depth']
    total_data_depth = forecast_depth + retrospective_data_depth

    end = pendulum.today().date()
    start = (end - pd.DateOffset(forecast_depth)).date()

    # Запрос берет количество рефок на каждый день от дня предшествующего сегодняшнему
    # на определенное количество суток (150 по-умолчанию) до сегодня.
    query = f"""
              select 
                  data_extraction_date as date,
                  count(distinct contfullnumber) as cnt_quantity
              from {schema}.{table} rcc  
              where data_extraction_date between '{(start - pd.DateOffset(total_data_depth)).date()}'
                                             and '{end}'
                and (split_part(cnt_location, ' ',2)::int < 31 or split_part(cnt_location, ' ',2)::int > 46)
              group by data_extraction_date
              """

    config['extract']['src']['params']['query'] = query

    meta = extract(config=config, get_func=get_by_custom_query)

    return meta

def run_model(config):

    uri = os.getenv('DUMP_PATH')
    cwd_content = os.listdir(os.getcwd())
    print(f'CWD contents: {cwd_content}')
    print(f'Dump file path: {uri}')
    big_df = get_data(uri)
    # data = predict(df=big_df, config=config)
    data = big_df
    return data

def save_result(result, config):

    src_config = config['extract']['src']
    conn_id = src_config['conn_id']
    schema = src_config['schema']
    table = src_config['table']

    extractor = PandasExtractor(uri=f'{conn_id}:{schema}.{table}')
    extractor.output = result
    batch = Batch()
    batch.fill(extractor)
    dump_path = render_dump_path(config, extraction_timestamp=batch.meta['extraction_timestamp'])
    batch.dump(dump_path)

    with open(rf"{xcom_folder_path}\airflow_xcom.json", "w") as f:
        json.dump(batch.meta, f, cls=DateTimeEncoder)


if __name__ == '__main__':

    dag_id = 'stg_isct_pred_ref_cnt_wa_ref_connections'
    CONFIG = get_config('../../../external/config/isct/predictive')[dag_id]

    extracted_meta = extract_and_dump(config=CONFIG)
    print(extracted_meta)
    os.environ['DUMP_PATH'] = extracted_meta['dump_uri']

    result_df = run_model(config=CONFIG)
    save_result(result=result_df, config=CONFIG)
    print(result_df)



