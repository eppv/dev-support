from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from external.utils.sql.common import sql_execute, prog_exc
from external.utils.var import color


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
    except prog_exc as exc:
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
    except prog_exc as exc:
        print(f'Cannot execute query on {engine.engine}. Programming error.')
        raise exc
    print(f'Columns: \n{columns} ({dtype}) \nadded to tables: \n{tables}')


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
    except prog_exc as exc:
        print(f'Cannot execute query on {engine.engine}. Programming error.')
        raise exc
    print(f'Columns successfully renamed.')
