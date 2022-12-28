
from sqlalchemy import create_engine
from external.utils.connections import Connection


def get_engine(conn_id):
    dsn = Connection.from_config(conn_id).render_db_dsn()
    engine = create_engine(dsn)
    return engine
