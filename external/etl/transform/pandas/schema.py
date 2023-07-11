import pandas as pd
import re

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


def add_cols_with_group_values(df, group_info):
    df[group_info['index_col']] = get_matching_rows_indexes(df, group_info['condition'], group_info['src_column'])
    df[group_info['group_name']] = df[group_info['src_column']].where(df[group_info['index_col']] == 1, pd.NA)
    return df


def extract_groups(df, groups_conditions):
    for value in groups_conditions.values():
        df[value['group_name']] = extract_group(df, value)
        df = df[df[value['index_col']] == 0]
        df.drop(columns=[value['index_col']], inplace=True)
    return df


def extract_group(df, group_info):
    df = add_cols_with_group_values(df, group_info)
    df[group_info['group_name']].ffill(inplace=True)
    return list(df[group_info['group_name']])


def unpivot_by_regex_args(df, unpivot_cols_regex:str, value_cols_regex:str):
    df = pd.melt(df, id_vars=list(df.loc[:,df.columns.str.contains(unpivot_cols_regex, case=False)].columns), \
                     value_vars=list(df.loc[:,df.columns.str.contains(value_cols_regex, case=False)].columns))
    return df


def unpivot(df, unpivot_cols, value_cols):
    df = pd.melt(df, id_vars=unpivot_cols, value_vars=value_cols)
    return df


def ffill_cols(df, cols_list):
    for value in cols_list:
        df[value].ffill(inplace=True)
    return df


def clear_headers(df, substr_to_change: str,  new_substr: str):
    df.columns = df.columns.str.replace(substr_to_change, new_substr)
    return df


def change_multi_level_header_row_type(df, row_info):
    for key, value in row_info.items():
        df.columns = df.columns.set_levels(df.columns.levels[key].astype(value['type']), level=key)
    return df


def flatten_cols(df: pd.DataFrame, delim: str = ""):
    new_cols = [delim.join((col_lev for col_lev in tup if col_lev))
                for tup in df.columns.values]
    df.columns = new_cols
    return df


def drop_columns(df, columns):
    df.drop(columns=columns, inplace=True)
    return df


def drop_cols_by_regex(df, condition):
    df = df.loc[:,~df.columns.str.contains(condition, case=False)]
    return df


def separate_df_cols_by_delim(df, col_params):
    for value in col_params.values():
        df[value['new_col_names']] = df[value['col_name']].str.split(value['delim'], expand=True)
        df.drop(columns=[value['col_name']], inplace=True)
    return df



