import os

from external.utils.exchange_server.account import get_account
from external.utils.var import color, to_datetime_string
from external.etl.file import excel
from exchangelib import FileAttachment
from datetime import datetime
from pytz import timezone

def get_items(account, folder, received_start, received_end):
    inbox = account.inbox
    src_folder = inbox.glob(f'**/{folder}')
    filtered_by_datetime_received = src_folder.filter(datetime_received__range=(received_start, received_end))

    items = []
    print('The following items found:')
    for item in filtered_by_datetime_received:
        print(f'Type: {item.ELEMENT_NAME}, Subject:{item.subject}')
        items.append(item)

    return items


def get_excel_attachments(item):
    excel_attachments = []
    for attachment in item.attachments:
        if isinstance(attachment, FileAttachment) and attachment.name.endswith(('.xlsx', '.xls')):
            print(f'Attachment: {attachment.name}')
            excel_attachments.append(attachment)
            return excel_attachments


def excel_extract_from_attachment(attachment, config, transform=None):
    from witness import Batch
    from witness.providers.pandas.extractors import PandasExcelExtractor

    src_config = config['extract']['src']
    sheet_name = src_config.setdefault('sheet_name', 0)
    header = src_config.setdefault('header', 0)
    dump = config['dump']
    record_source_array = [src_config['mail']['server'],
                           src_config['mail']['folder'],
                           attachment.name,
                           to_datetime_string(attachment.last_modified_time)]

    print(attachment.last_modified_time)

    extractor = PandasExcelExtractor(uri=attachment.content,
                                     sheet_name=sheet_name,
                                     header=header,
                                     dtype='str')

    if transform is not None:
        tsf_config = config['transform']
    else:
        tsf_config = None

    output = excel.extract_and_normalize(extractor, tsf_config=tsf_config, transform=transform)
    batch = Batch(data=output['data'], meta=output['meta'])
    batch.meta['record_source'] = '/'.join(record_source_array)

    extraction_ts_str = batch.meta['extraction_timestamp'].strftime('%Y-%m-%d_%H-%M-%S_%f')
    dump_path = f"{dump}/dump_{src_config['mail']['folder']}_{extraction_ts_str}"

    batch.dump(dump_path)
    print(f'+ Batch from {color(attachment.name, "yellow")} extracted and persisted.')

    return batch.meta




def extract(config, transform=None):
    src_config = config['extract']['src']

    my_account = get_account(server=src_config['mail']['server'],
                             login=src_config['mail']['login'],
                             password=src_config['mail']['password'])
    target_folder = src_config['mail']['folder']
    local_tz_name = os.getenv('AIRFLOW__CORE__DEFAULT_TIMEZONE')
    local_tz = timezone(local_tz_name)

    # received_start = src_config['mail']['received']['start']
    # received_end = src_config['mail']['received']['end']

    received_start = src_config['mail']['received']['start']
    received_end = src_config['mail']['received']['end']

    received_start_dt = datetime.fromisoformat(received_start).astimezone(local_tz)
    received_end_dt = datetime.fromisoformat(received_end).astimezone(local_tz)
    print(f'String start: {received_start},tz aware dt: {received_start_dt}')

    messages = get_items(my_account,
                         folder=target_folder,
                         received_start=received_start_dt,
                         received_end=received_end_dt)

    attachments_to_extract = []
    for msg in messages:
        attachments_to_extract.extend(get_excel_attachments(msg))


    extracted = []

    for attachment in attachments_to_extract:
        meta = excel_extract_from_attachment(attachment, config, transform=transform)
        extracted.append(meta)

    return extracted
