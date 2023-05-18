

def drop_na_rows(df, columns_to_check):
    df.dropna(subset=columns_to_check, inplace=True)
    return df
