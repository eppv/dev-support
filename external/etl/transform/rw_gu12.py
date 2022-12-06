

def define_headers(df, search_col_name, search_col_idx=0):
    header_idx = df[df.iloc[:, search_col_idx].eq(search_col_name)].index.values[0]
    df.columns = df.iloc[header_idx]
    df = df[header_idx + 1:]

    return df


def transform(df, config):
    column_name = config['column_name']
    columns_list = config['columns_list']
    df_with_headers = define_headers(df, search_col_name=column_name)
    df_with_headers = df_with_headers[columns_list]
    df_with_headers.dropna(thresh=3, inplace=True)

    return df_with_headers
