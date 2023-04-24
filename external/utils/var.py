import datetime
import pendulum
import os
import re


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
        dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        return dt_str
    else:
        return dt


def extract_datetime_string_from_filename(filename):
    datetime_regexes = [
        r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}',  # YYYY-MM-DD_HH-MM-SS
        r'\d{4}\d{2}\d{2}_\d{2}\d{2}\d{2}',  # YYYYMMDD_HHMMSS
        r'\d{4}\d{2}\d{2}_\d{2}\d{2}',  # YYYYMMDD_HHMM
        r'\d{2}.\d{2}.\d{4} \d{2}:\d{2}:\d{2}',  # DD.MM.YYYY HH:MM:SS
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
        r'\d{2}.\d{2}.\d{4}',  # DD.MM.YYYY
        r'\d{4}\d{2}\d{2}',  # YYYYMMDD
    ]

    # search for datetime string in filename using each regex pattern
    for regex in datetime_regexes:
        match = re.search(regex, filename)
        if match:
            datetime_str = match.group(0)
            return datetime_str

    return None


def extract_datetime_from_path(path):
    filename = os.path.basename(path)
    datetime_str = extract_datetime_string_from_filename(filename)

    datetime_formats = [
        '%Y-%m-%d %H:%M:%S',  # YYYY-MM-DD HH:MM:SS
        '%Y-%m-%d_%H-%M-%S',  # YYYY-MM-DD_HH-MM-SS
        '%Y%m%d_%H%M%S',  # YYYYMMDD_HHMMSS
        '%Y%m%d_%H%M',  # YYYYMMDD_HHMM
        '%d.%m.%Y %H:%M:%S',  # DD.MM.YYYY HH:MM:SS
        '%Y-%m-%d',  # YYYY-MM-DD
        '%d-%m-%Y',  # DD-MM-YYYY
        '%d.%m.%Y',  # DD.MM.YYYY
        '%Y%m%d',  # YYYYMMDD

    ]

    for format_str in datetime_formats:
        try:
            datetime_obj = datetime.datetime.strptime(datetime_str, format_str)
            return datetime_obj
        except ValueError:
            continue
    return None
