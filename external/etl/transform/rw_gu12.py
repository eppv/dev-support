import pandas as pd
from external.etl.transform.pandas import schema, rows

pd.set_option('mode.chained_assignment', None)

def rename_column(column_name, config):
    incorrect_names = config['column_rename']['incorrect_column_names']
    correct_name = config['column_rename']['correct_column_name']

    if column_name in incorrect_names:
        new_column_name = column_name.replace(column_name, correct_name)
        return new_column_name
    else:
        return column_name


def rename_columns(df, config):
    mapper = {name: rename_column(name, config) for name in df.columns}
    df.rename(columns=mapper, inplace=True)
    df.rename(columns=str.strip, inplace=True)
    return df


def remove_rows_with_missing_values(df, config):
    columns_with_missing_values = config['columns_with_missing_values']
    df.dropna(subset=columns_with_missing_values, inplace=True)

    return df


def transform(df, transform_config):
    column_name = transform_config['column_name']
    columns_list = transform_config['columns_list']
    columns_to_check = transform_config['drop_na_rows']['columns_to_check']
    df_with_headers = schema.define_headers(df, search_col_name=column_name)
    df_with_cols_renamed = rename_columns(df_with_headers, transform_config)
    df_selected = df_with_cols_renamed[[column for column in df_with_cols_renamed.columns if column in columns_list]]
    clean_df = rows.drop_na_rows(df_selected, columns_to_check=columns_to_check)

    return clean_df
