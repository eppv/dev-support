from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from external.utils.sql.common import ping_table, _throw_warning, sql_execute, sql_prog_exc

def truncate_table(engine, table, **kwargs):
    query = f'truncate table {table}'
    act = 'truncated'
    if ping_table(engine, table, **kwargs):
        _throw_warning(table, act=act)
        sql_execute(engine, query)
        print(f'Table {table} {act}.')


def drop_table_if_exists(engine, table, cascade=False, **kwargs):
    if cascade:
        query = f'drop table if exists {table} cascade'
    else:
        query = f'drop table if exists {table}'
    act = 'dropped'
    _throw_warning(table, act=act)
    sql_execute(engine, query)
    print(f'Table {table} {act}.')


def delete_records_by_period(engine, table, params):
    column = params['column']
    try:
        period = params['period']
        query = f"delete from {table} where {column} = '{period}';"
    except KeyError:
        start = params['start']
        end = params['end']
        query = f"delete from {table} where {column} between '{start}' and '{end}';"
    try:
        res = sql_execute(engine, query)
    except sql_prog_exc:
        print(f'Table {table} does not exist and will be created by loader.')



def drop_columns_from_multiple_tables(engine, tables: list, columns: list, cascade=False):
    session = sessionmaker(engine)()
    cascade_statement = ' cascade' if cascade else ''
    try:
        for table in tables:
            for column in columns:
                query = f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column}{cascade_statement};"
                session.execute(text(query))
        session.commit()
    except sql_prog_exc as exc:
        print(f'Cannot execute query on {engine.engine}. Programming error.')
        raise exc
    print(f'Columns: \n{columns} \ndropped from tables: \n{tables}')
