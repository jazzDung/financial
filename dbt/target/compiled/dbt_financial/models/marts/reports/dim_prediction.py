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

    stock_df.rename(columns={"time_stamp": "ds", "price": "y"}, inplace=True)
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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"int_prediction_input": "\"financial_data\".\"intermediate\".\"int_prediction_input\""}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "financial_data"
    schema = "marts"
    identifier = "dim_prediction"
    
    def __repr__(self):
        return '"financial_data"."marts"."dim_prediction"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


