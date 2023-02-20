
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
        cursor = session.execute(text(query))
        result = cursor.fetchall()
        session.commit()
        return result
    except sqlalchemy.exc.ResourceClosedError:
        cursor = session.execute(text(query))
        session.commit()
        return cursor
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
        res = sql_execute(engine, query)
        print(f'Table check on selecting record_source: {res}')
        return True
    except sql_prog_exc as exc:
        print(f'Table {table} does not exists')
        raise exc


def dummy_db_action(engine, table, **kwargs):
    print(f'Connecting database with {engine.engine}')
    print(table)
