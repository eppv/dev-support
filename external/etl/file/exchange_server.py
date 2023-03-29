from external.utils.exchange_server.account import get_account
from external.utils.configparse import get_config
from exchangelib import FileAttachment
from datetime import datetime
import pandas as pd


def get_items(account, folder, received_start, received_end):
    inbox = account.inbox
    tz = account.default_timezone
    src_folder = inbox.glob(f'**/{folder}')
    filtered_by_datetime_received = src_folder.filter(datetime_received__range=(received_start, received_end))

    items = []
    print('The following items found:')
    for item in filtered_by_datetime_received:
        received = item.datetime_received.astimezone(tz=tz)
        print(f'Type: {item.ELEMENT_NAME}, Subject:{item.subject}')
        items.append(item)

    return items


def get_excel_attachments(item):
    excel_attachments = []
    for attachment in item.attachments:
        if isinstance(attachment, FileAttachment) and attachment.name.endswith(('.xlsx', '.xls')):
            print(f'Attachment: {attachment.name}')
            excel_attachments.append(attachment.content)
            return excel_attachments


def extract(config):
    src_config = config['extract']['src']

    my_account = get_account(server=src_config['mail']['server'],
                             login=src_config['mail']['login'],
                             password=src_config['mail']['password'])
    target_folder = src_config['mail']['folder']
    acc_tz = my_account.default_timezone
    start = datetime(2023, 3, 29, tzinfo=acc_tz)
    end = datetime(2023, 3, 30, tzinfo=acc_tz)

    messages = get_items(my_account, folder=target_folder, received_start=start, received_end=end)
    files_to_extract = []
    for msg in messages:
        files_to_extract.extend(get_excel_attachments(msg))

    return [pd.read_excel(file, header=5) for file in files_to_extract]


if __name__ == '__main__':

    dag_id = 'raw_1c_prod_rep_cargo_forwarded_by_vmtp'
    CONFIG = get_config('../../config/1c_prod/cargo_forwarded_by_vmtp')[dag_id]
    dfs = extract(CONFIG)

