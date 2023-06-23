from external.etl.transform.pandas import schema, rows

REGISTRY = {
        'rename_columns': schema.rename_columns,
        'select_columns': schema.select_columns,
        'drop_na_rows': rows.drop_na_rows,
        'drop_na_cols': schema.drop_na_cols,
        'define_headers': schema.define_headers,
        'filter_by_conditions': rows.filter_by_conditions,
        'extract_groups': schema.extract_groups,
        'unpivot': schema.unpivot,
        'clear_headers': schema.clear_headers,
        'ffill_cols': schema.ffill_cols,
        'separate_df_cols_by_delim': schema.separate_df_cols_by_delim,
        'drop_columns': schema.drop_columns,
        'flatten_cols': schema.flatten_cols,
        'change_multi_level_header_row_type': schema.change_multi_level_header_row_type,
        'drop_cols_by_regex': schema.drop_cols_by_regex
}
