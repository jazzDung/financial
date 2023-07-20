
  create view "financial_data"."intermediate"."int_bollinger__dbt_tmp"
    
    
  as (
    with price_history as (
    select
        ticker,
        close,
        trading_date
    from
        "financial_data"."staging"."stg_price_history"
),
moving_average as (
    select
        *,
        AVG(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 5 PRECEDING
                AND CURRENT ROW
        ) AS sma_5,
        AVG(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 10 PRECEDING
                AND CURRENT ROW
        ) AS sma_10,
        AVG(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 20 PRECEDING
                AND CURRENT ROW
        ) AS sma_20,
        AVG(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 50 PRECEDING
                AND CURRENT ROW
        ) AS sma_50,
        AVG(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 100 PRECEDING
                AND CURRENT ROW
        ) AS sma_100,
        AVG(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 200 PRECEDING
                AND CURRENT ROW
        ) AS sma_200
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
        moving_average
),
bollinger as (
    select
        *,
        sma_5 as mid_band,
        sma_5 + (2 * std_dev) as upper_band,
        sma_5 - (2 * std_dev) as lower_band
    from
        standard_deviation
)
select
    ticker,
    close,
    case
        when lower_band is null then 0
        else round(CAST(lower_band as numeric), 2)
    end as lower_band,
    round(CAST(mid_band as numeric), 2) as mid_band,
    case
        when upper_band is null then 0
        else round(CAST(upper_band as numeric), 2)
    end as upper_band,
    trading_date
from
    bollinger
ORDER BY
    ticker,
    trading_date desc
  );