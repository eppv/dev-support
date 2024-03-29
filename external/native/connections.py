import os
from dataclasses import dataclass
from typing import Optional
from sqlalchemy import create_engine
from external.utils.configparse import read_yaml


EXTERNAL_MODULES_PATH = os.environ.get('EXTERNAL_MODULES_PATH')
default_config_path = os.path.abspath(f'{EXTERNAL_MODULES_PATH}/config/connections.yml')


@dataclass
class Connection:

    id: str
    conn_type: str
    description: Optional[str]
    extra: Optional[str]
    host: str
    port: Optional[str]
    schema: Optional[str]
    login: [str]
    password: [str]

    @classmethod
    def from_config(cls, conn_id, config_path=default_config_path):
        connection_configs = read_yaml(config_path)
        for config_id, config in connection_configs.items():
            if config_id == conn_id:
                return cls(id=config_id, **config)
        print('Connection id not found in the configuration file.')

    def render_db_dsn(self):
        dsn = f"{self.conn_type}://" \
              f"{self.login}:{self.password}" \
              f"@{self.host}:{self.port}" \
              f"/{self.schema}"

        return dsn


def get_engine(conn_id):
    dsn = Connection.from_config(conn_id).render_db_dsn()
    engine = create_engine(dsn)
    return engine
