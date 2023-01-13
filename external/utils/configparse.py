import os
import yaml


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


def get_etl_config(path):
    abs_config_path = os.path.abspath(path) + r'/config.yml'
    config = read_yaml(abs_config_path)['dags']
    return config
