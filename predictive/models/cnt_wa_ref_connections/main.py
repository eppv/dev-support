import os
import pendulum
import pandas as pd
from external.etl.sql import extract
from external.utils.configparse import get_config
from external.utils.sql.get import get_by_custom_query
from functions import run_model, save_result

xcom_folder_path = r"D:\data\tmp"
on_date = '2023-05-22'

on_date_obj = pendulum.parse(on_date)

def extract_and_dump(config):

    src_config = config['extract']['src']
    pred_config = config['transform']['predict']
    schema = src_config['schema']
    table = src_config['table']
    retrospective_data_depth = pred_config['retrospective_data_depth']
    end = pendulum.today().date()
    start = (end - pd.DateOffset(retrospective_data_depth)).date()

    # Запрос берет количество рефок на каждый день от дня предшествующего сегодняшнему
    # на определенное количество суток до сегодня.
    query = f"""
              select 
                  data_extraction_date as date,
                  count(distinct contfullnumber) as cnt_quantity
              from {schema}.{table} rcc  
              where data_extraction_date between '{start}'
                                             and '{end}'
                and (split_part(cnt_location, ' ',2)::int < 31 or split_part(cnt_location, ' ',2)::int > 46)
              group by data_extraction_date
              """

    config['extract']['src']['params']['query'] = query
    meta = extract(config=config, get_func=get_by_custom_query)
    return meta


def etl_container(dst: str, config: dict):

    extracted_meta = extract_and_dump(config=config)
    print(extracted_meta)
    os.environ['DUMP_PATH'] = extracted_meta['dump_uri']
    result_df = run_model(config=CONFIG)
    save_result(result=result_df, config=CONFIG)
    result_df.to_excel(rf'{xcom_folder_path}\{dst}')


if __name__ == '__main__':

    dag_id = 'stg_isct_pred_ref_cnt_wa_ref_connections'
    CONFIG = get_config('../../../external/config/isct/predictive')[dag_id]
    dst_file_name = f'model_result_optimized.xlsx'

    # etl_container(dst=dst_file_name, config=CONFIG)
    print(on_date_obj)
