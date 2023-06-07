
from external.etl.transform.rows import drop_na_rows

def transform(df, transform_config):

    columns_to_check = transform_config['drop_na_rows']['columns_to_check']
    clean_df = drop_na_rows(df, columns_to_check)

    return clean_df


