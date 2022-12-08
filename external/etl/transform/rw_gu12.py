

def define_headers(df, search_col_name, search_col_idx=0):
    header_idx = df[df.iloc[:, search_col_idx].eq(search_col_name)].index.values[0]
    df.columns = df.iloc[header_idx]
    df = df[header_idx + 1:]

    return df


def rename_col(col_name):
    import re
    if re.match(r'([А-Я][а-я]+\S[а-я]+\s[а-я]+\.)', col_name):
        new_col_name = re.sub('[А-Я][а-я]+\S[а-я]+\s[а-я]+\.\/[а-я]+\.', 'Кол-во ваг.', col_name)
        return new_col_name
    else:
        return col_name


def transform(df, config):
    column_name = config['column_name']
    columns_list = config['columns_list']
    df_with_headers = define_headers(df, search_col_name=column_name)
    df_with_headers.rename(columns=rename_col, inplace=True)
    df_with_headers = df_with_headers[columns_list]
    df_with_headers.dropna(thresh=3, inplace=True)

    return df_with_headers
