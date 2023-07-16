
  
    

  create  table "financial_data"."financial_clean"."dim_bollinger__dbt_tmp"
  
  
    as
  
  (
    with price_history as (
    select
        ticker,
        close,
        trading_date
    from
		"financial_data"."financial_clean"."dim_price_history"
),
sma as (
    select
        *,
        AVG(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 20 PRECEDING
                AND CURRENT ROW
        ) AS sma
    from
        price_history
),
standard_deviation as (
    select
        *,
        stddev(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 20 PRECEDING
                AND CURRENT ROW
        ) AS std_dev
    from
        sma
),
bollinger as (
    select
        *,
        sma as mid_band,
        sma + (2 * std_dev) as upper_band,
        sma - (2 * std_dev) as lower_band
    from
        standard_deviation
)
select
    ticker,
    close,
    lower_band,
    mid_band,
    upper_band,
    trading_date
from
    bollinger
ORDER BY
    ticker,
    trading_date desc
  );
  