from dataclasses import dataclass
from typing import Optional
from os.path import abspath
from external.utils.configparse import read_yaml

config_path = abspath('../config/connections.yml')


@dataclass
class Connection:

    id: str
    type: str
    host: str
    port: Optional[str]
    schema: Optional[str]
    login: [str]
    password: [str]

    @classmethod
    def from_config(cls, conn_id):
        connection_configs = read_yaml(config_path)['connections']
        for config in connection_configs:
            if config['id'] == conn_id:
                return cls(**config)
        print('No connection with such id defined in configuration file')

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



