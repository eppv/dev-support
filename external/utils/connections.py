
from external.utils.var import read_yaml


def render_dsn(config):
    host = config['host']
    port = config['port']
    dialect = config['dialect']
    user = config['user']
    pwd = config['password']
    db = config['db']
    dsn = f"{dialect}://" \
          f"{user}:{pwd}" \
          f"@{host}:{port}" \
          f"/{db}"
    return dsn


conn_config_path = r''
conf = read_yaml(conn_config_path)
