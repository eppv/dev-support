from dataclasses import dataclass
from typing import Optional
from os.path import abspath
from external.utils.var import read_yaml

config_path = abspath('../config/connections.yml')


@dataclass
class Connection:

    def __init__(self, config: Optional[dict]):

        self.id: str = config['id']
        self.type: str = config['type']
        self.host: str = config['host']
        self.port: int = config['port']
        self.schema: Optional[str] = config['schema']
        self.login: Optional[str] = config['login']
        self.password: Optional[str] = config['password']

    def render_db_dsn(self):
        host = self.host
        port = self.port
        dialect = self.type
        user = self.login
        pwd = self.password
        db = self.schema
        dsn = f"{dialect}://" \
              f"{user}:{pwd}" \
              f"@{host}:{port}" \
              f"/{db}"

        return dsn


def get_connection(conn_id):
    connection_configs = read_yaml(config_path)['connections']
    for config in connection_configs:
        if config['id'] == conn_id:
            return Connection(config)
