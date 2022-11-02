
from external.utils.connections import get_connection


def test_get_connection():

    conn_id = 'main_dwh'
    conn = get_connection(conn_id)
    assert conn.id == conn_id


def test_render_db_dsn():
    conn_id = 'main_dwh'
    conn = get_connection(conn_id)
    dsn = conn.render_db_dsn()
    print(dsn)

