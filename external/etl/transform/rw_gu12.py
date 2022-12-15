import pandas as pd

pd.set_option('mode.chained_assignment', None)


def define_headers(df, search_col_name, search_col_idx=0):
    df_first_column = df.iloc[:, search_col_idx]
    headers_row = df_first_column.eq(search_col_name)
    header_idx = df[headers_row].index.values[0]
    df.columns = df.iloc[header_idx]
    df = df[header_idx + 1:]

    return df


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
    return df


def transform(df, transform_config):
    column_name = transform_config['column_name']
    columns_list = transform_config['columns_list']
    df_with_headers = define_headers(df, search_col_name=column_name)
    df_with_cols_renamed = rename_columns(df_with_headers, transform_config)
    df_selected = df_with_cols_renamed[columns_list]
    df_selected.dropna(thresh=3, inplace=True)
    return df_selected
