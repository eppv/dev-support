
from typing import Optional
from external.etl.transform.schema import rename_columns, select_columns, drop_na_cols, define_headers
from external.etl.transform.rows import drop_na_rows

REGISTRY = {
    'rename_columns': rename_columns,
    'select_columns': select_columns,
    'drop_na_rows': drop_na_rows,
    'drop_na_cols': drop_na_cols,
    'define_headers': define_headers
}


class Scenario:

    """
    sequence - a list of transform function names
    config - a transform section of the config file
    """

    def __init__(self, sequence, config=None):

        self.sequence: list[callable] = sequence
        self.config: Optional[dict] = config

    def apply(self, df):
        for func_name in self.sequence:
            if self.config[func_name] is None:
                df = REGISTRY[func_name](df)
            else:
                df = REGISTRY[func_name](df, **self.config[func_name])

        return df



