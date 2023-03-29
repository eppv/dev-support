from external.utils.configparse import get_config


def test_get_config():
    path = '../config/iac_forms'
    config = get_config(path)
    print(config)

