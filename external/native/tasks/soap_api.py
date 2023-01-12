
from witness import Batch
from witness.extractors.http import HttpGetExtractor
from external.native.connections import Connection
from external.utils.var import render_dump_path


def parse_soap_xml(rxml, config):
    from xml.etree import ElementTree

    namespaces = {
        'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
        'report_ns': 'http://tempuri.org'
    }
    soap_tree = ElementTree.fromstring(rxml)
    soap_method = config['extract']['src']['params']['soap_method']
    response_tag = f'{soap_method}Response'
    result_tag = f'{soap_method}Result'
    entity = config['extract']['src']['entity']

    tree_elements = soap_tree.findall(
        './soap:Body'
        f'/report_ns:{response_tag}'
        f'/report_ns:{result_tag}'
        '/report_ns:Lines'
        f'/report_ns:{entity}',
        namespaces,
    )
    list_data = []
    for element in tree_elements:
        row = {}
        for field in element.iter():
            row[field.tag[20:]] = field.text
        list_data.append(row)
    return list_data


def get_extract(config):
    src_conn = Connection.from_config(config['extract']['src']['conn_id'])
    params = config['extract']['src']['params']

    extractor = HttpGetExtractor(uri=src_conn.host, params=params)
    response = extractor.extract().output
    response.raise_for_status()
    raw_xml = response.content

    dump_path = render_dump_path(config, extractor.extraction_timestamp)
    meta = {'extraction_timestamp': extractor.extraction_timestamp,
            'record_source': f"{extractor.uri}?{params['soap_method']}"}

    data = parse_soap_xml(raw_xml, config)
    batch = Batch(meta=meta, data=data)
    batch.dump(dump_path)

    return batch.meta

