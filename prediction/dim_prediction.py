import pandas as pd
import time
import logging
from os import cpu_count

from tqdm import tqdm
from pathos.multiprocessing import ProcessingPool as Pool
from prophet import Prophet

logger = logging.getLogger("fbprophet.plot")
logger.setLevel(logging.CRITICAL)


def run_prophet(input):
    ticker, time_series = input
    m = Prophet(
        interval_width=0.90,
        growth="linear",
        changepoint_prior_scale=0.1,
        n_changepoints=30,
        seasonality_prior_scale=15,
        seasonality_mode="multiplicative",
    )
    m.fit(time_series)
    future = m.make_future_dataframe(periods=7 * 4, freq="d", include_history=False)
    forecast = m.predict(future)
    forecast["ticker"] = ticker
    return forecast


def model(dbt, fal):
    dbt.config(fal_environment="predicting")

    stock_df: pd.DataFrame = dbt.ref("int_prediction_input")

    stock_df.rename(columns={"trading_date": "ds", "price": "y"}, inplace=True)
    stocks = tuple(stock_df.groupby("ticker"))

    start_time = time.time()
    # p = Pool(cpu_count())
    print("cpu count: {count}".format(count=cpu_count()))
    p = Pool(4)
    predictions = list(tqdm(p.imap(run_prophet, stocks), total=len(stocks)))
    p.close()
    p.join()
    print("--- %s seconds ---" % (time.time() - start_time))

    to_db = pd.concat(predictions)

    print("Uploading\n")
    return to_db
