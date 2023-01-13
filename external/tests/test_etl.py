from external.utils.configparse import get_etl_config


def test_get_config():
    path = '../config/iac_forms'
    config = get_etl_config(path)
    print(config)

