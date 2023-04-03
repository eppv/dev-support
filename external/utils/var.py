import datetime
import pendulum


def params_to_str(http_params: dict) -> str:
    str_params = ''
    for key, value in http_params.items():
        str_params = str_params + f'{str(key)}={str(value)}&'
    return str_params


def color(string: str, color_name: str = 'white') -> str:

    pref = '\033['
    reset = f'{pref}0m'

    ansi_codes = {
        'red': '31m',
        'green': '32m',
        'yellow': '33m',
        'blue': '34m',
        'magenta': '35m',
        'cyan': '36m',
        'white': '37m',
        'reset': '0m'
    }

    return f'{pref}{ansi_codes[color_name]}{string}{reset}'


def render_dump_path(config, extraction_timestamp):
    ts_string = extraction_timestamp.strftime('%Y-%m-%d_%H-%M-%S_%f')
    uri = f"{config['load']['sink']['schema']}_{config['load']['sink']['table']}"
    dump_path = f"{config['dump']}/dump_{uri}_{ts_string}"
    return dump_path


def to_datetime_string(dt):

    if isinstance(dt, pendulum.DateTime):
        dt_str = dt.to_datetime_string()
        return dt_str
    elif isinstance(dt, datetime.datetime):
        dt_str = dt.strftime('%Y-%m-%d %H-%M-%S')
        return dt_str
