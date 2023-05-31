import datetime


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


def to_datetime_string(dt):

    if isinstance(dt, datetime.datetime):
        dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        return dt_str
    else:
        return dt
