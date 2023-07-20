with price_history as (
    select
        ticker,
        close,
        trading_date
    from
        {{ ref('stg_price_history') }}
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