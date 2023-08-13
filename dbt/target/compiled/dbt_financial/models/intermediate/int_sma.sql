with price_history as (
    select
        *
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
)
select
    ticker,
    open,
    high,
    low,
    close,
    volume,
    round(CAST(sma_5 as numeric), 2) as sma_5,
    round(CAST(sma_10 as numeric), 2) as sma_10,
    round(CAST(sma_20 as numeric), 2) as sma_20,
    round(CAST(sma_50 as numeric), 2) as sma_50,
    round(CAST(sma_100 as numeric), 2) as sma_100,
    round(CAST(sma_200 as numeric), 2) as sma_200,
    trading_date 
from
    moving_average
ORDER BY
    ticker,
    trading_date desc