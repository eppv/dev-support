
import warnings # предупреждения
import pandas as pd
import pendulum
from autots import AutoTS

warnings.simplefilter("ignore") # убирает предупреждения


def get_data(uri):
    data = pd.read_pickle(uri)
    df = pd.DataFrame(data)
    print('Got df from extracted and dumped data:')
    print(df)
    return df


def get_forecast_daterange(config):
    pred_config = config['transform']['predict']
    forecast_depth = pred_config['forecast_depth']

    end = pendulum.now().date()
    start = (end - pd.DateOffset(forecast_depth)).date()

    date_range = pd.date_range(start, end)
    return date_range

def get_forecast_df_range(df, config):
    forecast_daterange = get_forecast_daterange(config)

    forecast_df_range = []

    for forecast_date in forecast_daterange:
        forecast_start_date = forecast_date - pd.DateOffset(config['transform']['predict']['retrospective_data_depth'])

        print(f'Forecast start: {forecast_start_date}, forecast end: {forecast_date}')

        forecast_df = df[(df['date'] >= forecast_start_date.date()) & (df['date'] < forecast_date.date())]
        forecast_df_range.append(forecast_df)

    return forecast_df_range


def predict(df, config):

    forecasts_df = pd.DataFrame()
    forecasts_upper = pd.DataFrame()
    forecasts_lower = pd.DataFrame()

    long = False
    dfs_for_forecast = get_forecast_df_range(df, config)


    for fdf in dfs_for_forecast:
        fdf = fdf.set_index('date')
        print(fdf)
        fdf.index = fdf.index.astype('datetime64[ns]')
        print(fdf)
        model = AutoTS(
            forecast_length=21,
            frequency='infer',
            prediction_interval=0.9,
            ensemble=None,
            model_list="default",  # "superfast", "default", "fast_parallel"
            transformer_list="fast",  # "superfast",
            drop_most_recent=1,
            max_generations=4,
            num_validations=2,
            validation_method="backwards",
            min_allowed_train_percent=0.5
        )
        model = model.fit(
            fdf,
            date_col='datetime' if long else None,
            value_col='value' if long else None,
            id_col='series_id' if long else None,
        )

        prediction = model.predict()

        print(model)

        forecasts_df = pd.concat([forecasts_df, prediction.forecast])
        forecasts_upper = pd.concat([forecasts_upper, prediction.upper_forecast])
        forecasts_lower = pd.concat([forecasts_lower, prediction.lower_forecast])

    forecasts_df = forecasts_df.reset_index().rename(columns={'index': 'date'})
    forecasts_upper = forecasts_upper.reset_index().rename(columns={'index': 'date'})
    forecasts_lower = forecasts_lower.reset_index().rename(columns={'index': 'date'})


    # dataframe для заливки в БД с прогнозом
    result = pd.concat([forecasts_df.groupby('date').cnt_quantity.mean().rename('target'),
                        forecasts_upper.groupby('date').cnt_quantity.mean().rename('upper'),
                        forecasts_lower.groupby('date').cnt_quantity.mean().rename('lower'),
                        ]
                       , axis = 1).reset_index()

    return result

