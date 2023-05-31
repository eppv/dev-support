
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

