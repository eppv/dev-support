
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
    if ping_table(engine, table, **kwargs):
        _throw_warning(table, act='truncated')
        sql_execute(engine, query)
        print(f'Table {table} truncated.')


def drop_table_if_exists(engine, table, **kwargs):
    query = f'drop table if exists {table}'
    _throw_warning(table, act='dropped')
    sql_execute(engine, query)
    print(f'Table {table} truncated.')


def get_loaded_src_ids(engine, table, **kwargs):
    query = f'select distinct record_source from {table};'

    try:
        src_list = sql_execute(engine, query)
        names = [name for name, in src_list]
        return names
    except sqlalchemy.exc.ProgrammingError:
        print(f'Table "{color(table, "yellow")}" does not exist! Filenames list is empty!')
        return []


def grant_preset_priveleges(engine, table, **kwargs):
    query = f"""
        grant all privileges on table {table} to data_engineer, service;
        grant select on table {table} to analyst, power_bi;
    """
    try:
        sql_execute(engine, query)
        print(f'Priveleges granted on table {table}')
    except sqlalchemy.exc.ProgrammingError:
        print(f'Cannot connect to table {color(table, "yellow")}')
        return None


def dummy_db_action(engine, table, **kwargs):
    print(f'Connecting database with {engine.engine}')
    print(table)


def delete_records_by_period(engine, table, params):
    column = params['column']
    period = params['period']
    query = f"select {column} from {table} where {column} = '{period}';"
    res = sql_execute(engine, query).fetchall()
    print(res)


