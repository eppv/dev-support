
from typing import Optional
from external.etl.transform.pandas import REGISTRY as PANDAS_REGISTRY

REGISTRIES = {
    'pandas': PANDAS_REGISTRY
}


class Scenario:

    """
    sequence - a list of transform function names
    config - a transform section of the config file
    """

    def __init__(self,
                 sequence: list[callable],
                 config: Optional[dict] = None,
                 registry: str = 'pandas'):

        self.sequence: list[callable] = sequence
        self.config: Optional[dict] = config
        self.registry = REGISTRIES[registry]

    def apply(self, df):
        if self.config is None:
            return df

        for func_name in self.sequence:
            if self.config[func_name] is None:
                df = self.registry[func_name](df)
            else:
                df = self.registry[func_name](df, **self.config[func_name])

        return df



