from witness import Batch
from witness.providers.pandas.extractors import PandasExcelExtractor
from witness.providers.pandas.loaders import PandasExcelLoader
from external.utils.var import show_exec_time
from datetime import datetime

test_path = r'\\Vld-fs01\work\КД\Общие файлы КД\ИАЦ\8. Исходники\1С\CRM\Ежедневный_отчет_ОЭ'
input_filename = 'Таблица слежения для ИАЦ вер2 (XLSX)'
output_filename = 'crm_tracking_table'
dump_path = r'C:\data\dump'


@show_exec_time
def extract(extractor, dump):
    batch = Batch()
    print(f'Starting at {str(datetime.now())}')
    print(f'Extracting from {extractor.uri}...')
    batch.fill(extractor)
    print('Extracted.')
    batch.dump(dump)
    print(f'Dumpfile created ({dump})')
    return batch.meta


@show_exec_time
def load(meta, loader):
    batch = Batch(meta=meta)
    print(f'Starting restoring batch at {str(datetime.now())}')
    batch.restore()
    print('Restored.')
    batch.push(loader, meta_elements=['extraction_timestamp'])
    print(f'Loaded to {loader.uri}')


if __name__ == '__main__':

    xl_extractor = PandasExcelExtractor(rf'{test_path}\{input_filename}.xlsx', header=3)
    batch_meta = extract(xl_extractor, dump_path)

    xl_loader = PandasExcelLoader(rf'{test_path}\{output_filename}.xlsx')
    load(batch_meta, xl_loader)
