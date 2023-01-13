
import sqlalchemy.exc
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from external.utils.var import color
from time import sleep
from typing import Optional


sql_prog_exc = sqlalchemy.exc.ProgrammingError


def _throw_warning(table, act: str):
    warning_msg = f'{color("WARNING:", "red")} table {color(table, "yellow")} will be {act}!'
    print(warning_msg)
    for i in range(5):
        print(f'{5-i} seconds remains...')
        sleep(1)
    print('Running...')


def sql_execute(engine, query):
    session = sessionmaker(engine)()
    try:
        result = session.execute(text(query))
        session.commit()
        return result
    except sql_prog_exc as exc:
        print(f'Cannot execute query on {engine.engine}. Programming error.')
        raise exc


def apply_on_db(engine, table, actions: Optional[list] = None, **kwargs):
    if actions is None:
        return
    for executable in actions:
        executable(engine, table, **kwargs)


def ping_table(engine, table, **kwargs):
    query = f"select record_source from {table} limit 1;"
    try:
        res = sql_execute(engine, query).fetchall()
        print(f'Table check on selecting record_source: {res}')
        return True
    except sql_prog_exc as exc:
        print(f'Table {table} does not exists')
        raise exc


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


def dummy_db_action(engine, table, **kwargs):
    print(f'Connecting database with {engine.engine}')
    print(table)


def delete_records_by_period(engine, table, params):
    column = params['column']
    period = params['period']
    query = f"select {column} from {table} where {column} = '{period}';"
    res = sql_execute(engine, query).fetchall()
    print(res)


def add_column(engine, table, column: str, dtype='text', default=None):
    query = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {dtype} DEFAULT {default};"
    sql_execute(engine, query=query)
    print(f'Column {column} ({dtype}) added to table {table}')


def add_columns(engine, table, columns: list, dtype='text', default=None):
    session = sessionmaker(engine)()
    try:
        for column in columns:
            query = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {dtype} DEFAULT {default};"
            session.execute(text(query))
        session.commit()
    except sql_prog_exc as exc:
        print(f'Cannot execute query on {engine.engine}. Programming error.')
        raise exc
    print(f'Columns {columns} ({dtype}) added to table {table}')


def add_columns_to_multiple_tables(engine, tables: list, columns: list, dtype='text', default=None):
    session = sessionmaker(engine)()
    try:
        for table in tables:
            for column in columns:
                query = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {dtype} DEFAULT {default};"
                session.execute(text(query))
        session.commit()
    except sql_prog_exc as exc:
        print(f'Cannot execute query on {engine.engine}. Programming error.')
        raise exc
    print(f'Columns: \n{columns} ({dtype}) \nadded to tables: \n{tables}')


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


def add_load_timestamp(engine, table, **kwargs):
    query = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS load_timestamp timestamp DEFAULT now();"
    sql_execute(engine, query=query)
    print(f'Column {color("load_timestamp", "yellow")} added to table {table} '
          f'and set default for now()')


def rename_columns(engine, table, mapping: dict, **kwargs):
    session = sessionmaker(engine)()
    try:
        for old_name, new_name in mapping.items():
                query = f"""
                        ALTER TABLE {table} 
                        RENAME COLUMN {old_name} TO {new_name};
                    """
                session.execute(text(query))
        session.commit()
    except sql_prog_exc as exc:
        print(f'Cannot execute query on {engine.engine}. Programming error.')
        raise exc
    print(f'Columns successfully renamed.')
