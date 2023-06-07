
from external.etl.transform.pandas import schema, rows

REGISTRY = {
        'rename_columns': schema.rename_columns,
        'select_columns': schema.select_columns,
        'drop_na_rows': rows.drop_na_rows,
        'drop_na_cols': schema.drop_na_cols,
        'define_headers': schema.define_headers,
        'filter_by_conditions': rows.filter_by_conditions,
        'extract_group': schema.extract_group,
        'table_unpivoting': schema.unpivot,
        'clear_headers': schema.clear_headers
}
