import sqlalchemy.exc

from external.utils.sql.common import execute
from external.utils.var import color


def preset_priveleges(engine, table, **kwargs):
    query = f"""
        grant all privileges on table {table} to data_engineer, service;
        grant select on table {table} to analyst, power_bi;
    """
    try:
        execute(engine, query)
        print(f'Priveleges granted on table {table}')
    except sqlalchemy.exc.ProgrammingError:
        print(f'Cannot connect to table {color(table, "yellow")}')
        return None
