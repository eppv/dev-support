
from external.utils.sql.common import sql_execute


def get_by_period(engine, table, period_col, start, end, limit=None, **kwargs):

    limit_exp = '' if limit is None else f'limit {limit}'

    query = f'select * ' \
            f'from {table} ' \
            f'where {period_col} between \'{start}\' and \'{end}\'' \
            + limit_exp \
            + ';'
    result = sql_execute(engine, query).all()
    return result
