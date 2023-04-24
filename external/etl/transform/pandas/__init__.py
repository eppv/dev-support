
from external.etl.transform.pandas import schema
from external.etl.transform.pandas import rows

REGISTRY = {
        'rename_columns': schema.rename_columns,
        'select_columns': schema.select_columns,
        'drop_na_rows': rows.drop_na_rows,
        'drop_na_cols': schema.drop_na_cols,
        'define_headers': schema.define_headers
}
