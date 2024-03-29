
import os
from external.utils.sql.common import execute
from external.utils.configparse import read_query_file


def get_all(engine, table, limit=None, **kwargs):

    limit_exp = '' if limit is None else f'limit {limit}'

    query = f'select * ' \
            f'from {table} ' \
            + limit_exp \
            + ';'
    result = execute(engine, query)

    return result


def get_by_period(engine, table, period_col, start, end, limit=None, **kwargs):

    limit_exp = '' if limit is None else f'limit {limit}'

    query = f'select * ' \
            f'from {table} ' \
            f'where {period_col} between \'{start}\' and \'{end}\'' \
            + limit_exp \
            + ';'
    result = execute(engine, query)

    return result


def get_by_custom_query(engine, query, **kwargs):

    if os.path.exists(query):
        query = read_query_file(query)

    result = execute(engine, query)
    return result