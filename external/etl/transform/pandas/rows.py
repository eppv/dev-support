

def drop_na_rows(df, columns_to_check):
    df.dropna(subset=columns_to_check, inplace=True)
    return df


def filter_by_conditions(df, conditions):
    for column, condition_set in conditions.items():
        src_column = condition_set['src_column']
        condition = condition_set['condition']
        df = df[df[src_column].str.match(condition) != True]

    return df