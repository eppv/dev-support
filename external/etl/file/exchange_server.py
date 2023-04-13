import os

from external.utils.exchange_server.account import get_account
from external.utils.var import color, to_datetime_string
from external.etl.file import excel
from exchangelib import FileAttachment
from datetime import datetime
from pytz import timezone

LOCAL_TZ_NAME = os.getenv('AIRFLOW__CORE__DEFAULT_TIMEZONE')
LOCAL_TZ = timezone(LOCAL_TZ_NAME)

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
            item_meta = {
                'item_subject': item.subject,
                'item_received': to_datetime_string(item.datetime_received.astimezone(LOCAL_TZ))
            }

            return excel_attachments, item_meta


def get_attachments_to_extract_with_meta(messages: list):
    attachments_with_meta_to_extract = {}
    for msg in messages:
        attachments, item_meta = get_excel_attachments(msg)
        for attachment in attachments:
            attachment_with_meta = {attachment: item_meta}
            attachments_with_meta_to_extract.update(attachment_with_meta)

    return attachments_with_meta_to_extract

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
                           to_datetime_string(attachment.last_modified_time.astimezone(LOCAL_TZ))]

    print(attachment.last_modified_time.astimezone(LOCAL_TZ))

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
    received_start = src_config['mail']['received']['start']
    received_end = src_config['mail']['received']['end']

    received_start_dt = datetime.fromisoformat(received_start).astimezone(LOCAL_TZ)
    received_end_dt = datetime.fromisoformat(received_end).astimezone(LOCAL_TZ)
    print(f'Received Start Datetime: {received_start}, Received Start Datetime TZ Aware: {received_start_dt}')

    messages = get_items(my_account,
                         folder=target_folder,
                         received_start=received_start_dt,
                         received_end=received_end_dt)

    attachments_to_extract_with_meta = get_attachments_to_extract_with_meta(messages)

    extracted_meta = []
    for attachment, item_meta in attachments_to_extract_with_meta.items():
        meta = excel_extract_from_attachment(attachment, config, transform=transform)
        meta.update(item_meta)
        extracted_meta.append(meta)

    return extracted_meta
