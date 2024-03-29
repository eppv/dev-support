import os
import yaml

EXTERNAL_MODULES_PATH = os.environ.get('EXTERNAL_MODULES_PATH')
DEFAULT_CONFIG_PATH = os.path.abspath(f'{EXTERNAL_MODULES_PATH}/config')
encodings = ('utf-8', 'cp1251', 'cp866', 'cp855', 'koi8_r', 'cyrillic', 'maccyrillic')


def read_yaml(filepath):
    for encoding in encodings:
        try:
            with open(filepath,
                      'r', encoding=encoding) as stream:
                try:
                    return yaml.safe_load(stream)
                except yaml.YAMLError as exc:
                    print(exc)
        except UnicodeDecodeError:
            continue


def read_query_file(filepath):
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding) as q:
                return q.read()
        except UnicodeDecodeError:
            continue


def get_config(path):
    abs_config_path = os.path.abspath(path) + r'/config.yml'
    config = read_yaml(abs_config_path)['dags']
    return config


def get_trf_config_and_sequence(config):
    try:
        trf_config = config['transform']
        trf_seq = [*trf_config.keys()]
    except KeyError:
        print('There is no transform config.')
        return None, None
    except AttributeError:
        print('Transform config is invalid.')
        return None, None
    return trf_config, trf_seq
