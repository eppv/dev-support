import datetime
import os
import re
from pathlib import Path


def get_filepaths(dirpath):
    dirpath = Path(dirpath)
    if not dirpath.exists():
        print(f"The provided path: {dirpath} does not exist.")
        return None
    files_roots = [file for file in dirpath.rglob('*')]
    return files_roots


def render_dump_path(config, extraction_timestamp):
    ts_string = extraction_timestamp.strftime('%Y-%m-%d_%H-%M-%S_%f')
    try:
        uri = f"{config['load']['sink']['schema']}_{config['load']['sink']['table']}"
    except KeyError:
        root, uri = os.path.split(config['load']['sink']['uri'])
    dump_path = f"{config['dump']}/dump_{uri}_{ts_string}"
    return dump_path


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
        r'\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}'  # YYYYMMDDHHMMSS
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
        '%Y%m%d%H%M%S'  # YYYYMMDDHHMMSS
    ]

    for format_str in datetime_formats:
        try:
            datetime_obj = datetime.datetime.strptime(datetime_str, format_str)
            return datetime_obj
        except ValueError:
            continue
    return None
