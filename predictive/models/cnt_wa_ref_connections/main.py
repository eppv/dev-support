
from functions import predict, extract, docker_build_img
from external.utils.configparse import get_config
from external import HOST_PROJ

def run_model(config):

    big_df = extract(config)
    data = predict(df=big_df, config=config)
    return data


if __name__ == '__main__':

    dag_id = 'stg_isct_pred_ref_cnt_wa_ref_connections'
    CONFIG = get_config('../../../external/config/isct/predictive')[dag_id]

    # result_df = run_model(config=CONFIG)

    command = docker_build_img(task_id='', config=CONFIG)
    print(command)



