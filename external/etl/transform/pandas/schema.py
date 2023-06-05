
import re
import pandas as pd

pd.set_option("mode.chained_assignment", None)


def define_headers(df, search_col_name, search_col_idx=0):
    df_first_column = df.iloc[:, search_col_idx]
    headers_row = df_first_column.eq(search_col_name)
    header_idx = df[headers_row].index.values[0]
    df.columns = df.iloc[header_idx]
    df = df[header_idx + 1:]

    return df


def drop_na_cols(df, *args, how='all'):
    df.dropna(axis=1, how=how, inplace=True)
    return df


def rename_columns(df, aliases_mapping):
    mapper = {}
    for unified_name, aliases in aliases_mapping.items():
        for name in df.columns:
            if name in aliases:
                mapper[name] = unified_name

    df.rename(columns=mapper, inplace=True)
    return df


def select_columns(df, columns):
    to_select = [column for column in df.columns if column in columns]
    df_selected = df.loc[:, to_select]
    return df_selected


def get_matching_rows_indexes(df, condition, column):
    indexes = [1 if isinstance(val, str) and re.search(condition, val) else 0 for val in df[column]]
    return indexes


def add_cols_with_group_values(df, group_conditions_in_col):
    for group in group_conditions_in_col.values():
        df[group['index_col']] = get_matching_rows_indexes(df, group['condition'], group['src_column'])
        df[group['group_name']] = df[group['src_column']].where(df[group['index_col']] == 1, None)

    return df


def extract_groups(df, params):
    for group in params:
        extract_group(df, group)
    pass


def extract_group(df, group_conditions_in_col):
    df = add_cols_with_group_values(df, group_conditions_in_col)

    for value in group_conditions_in_col.values():
        df[value['group_name']].ffill(inplace=True)
        df = df[df[value['index_col']] == 0]
        df.drop(columns=[value['index_col']], inplace=True)

    return df


def unpivot(df, unpivot_cols, value_cols, ffill_col_name=None):
    df = pd.melt(df, id_vars=unpivot_cols, value_vars=value_cols)

    if ffill_col_name is not None:
        df[ffill_col_name].ffill(inplace=True)

    return df


def clear_headers(df, substr_to_change: str,  new_substr: str):
    df.columns = df.columns.str.replace(substr_to_change, new_substr)
    return df

