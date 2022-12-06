
from external.utils.configparse import get_etl_config
from external.etl.file.directory import dir_extract, dir_load
from external.etl.dbt import dbt_run

if __name__ == '__main__':

    dag_id = 'iac_forms_cnt_working_area_comments'

    # CONFIG = get_etl_config('/opt/airflow/dags/ingestion/iac_forms')[dag_id]

    CONFIG = get_etl_config('../config/iac_forms')[dag_id]

    meta = dir_extract(CONFIG)

    dir_load(meta=meta, config=CONFIG)

    dbt_run(CONFIG)
