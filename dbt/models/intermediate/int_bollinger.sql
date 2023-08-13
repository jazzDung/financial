with
moving_average as (
    select
        * from {{ ref('int_sma') }}
),
standard_deviation as (
    select
        *,
        stddev(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 5 PRECEDING
                AND CURRENT ROW
        ) AS std_dev_5,
        stddev(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 10 PRECEDING
                AND CURRENT ROW
        ) AS std_dev_10,
        stddev(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 20 PRECEDING
                AND CURRENT ROW
        ) AS std_dev_20,
        stddev(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 50 PRECEDING
                AND CURRENT ROW
        ) AS std_dev_50,
        stddev(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 100 PRECEDING
                AND CURRENT ROW
        ) AS std_dev_100,
        stddev(close) OVER(
            partition by ticker
            ORDER BY
                ticker,
                trading_date asc ROWS BETWEEN 200 PRECEDING
                AND CURRENT ROW
        ) AS std_dev_200
    from
        moving_average
),
bollinger as (
    select
        *,
        sma_5 + (2 * std_dev_5) as upper_band_5,
        sma_5 - (2 * std_dev_5) as lower_band_5,
        sma_10 + (2 * std_dev_10) as upper_band_10,
        sma_10 - (2 * std_dev_10) as lower_band_10,
        sma_20 + (2 * std_dev_20) as upper_band_20,
        sma_20 - (2 * std_dev_20) as lower_band_20,
        sma_50 + (2 * std_dev_50) as upper_band_50,
        sma_50 - (2 * std_dev_50) as lower_band_50,
        sma_100 + (2 * std_dev_100) as upper_band_100,
        sma_100 - (2 * std_dev_100) as lower_band_100,
        sma_200 + (2 * std_dev_200) as upper_band_200,
        sma_200 - (2 * std_dev_200) as lower_band_200
    from
        standard_deviation
)
select
    ticker,
    open,
    high,
    low,
    close,
    volume,
    case
        when lower_band_5 is null then 0
        else round(CAST(lower_band_5 as numeric), 2)
    end as lower_band_5,
    sma_5,
    case
        when upper_band_5 is null then 0
        else round(CAST(upper_band_5 as numeric), 2)
    end as upper_band_5,
    case
        when lower_band_10 is null then 0
        else round(CAST(lower_band_10 as numeric), 2)
    end as lower_band_10,
    sma_10,
    case
        when upper_band_10 is null then 0
        else round(CAST(upper_band_10 as numeric), 2)
    end as upper_band_10,
    case
        when lower_band_20 is null then 0
        else round(CAST(lower_band_20 as numeric), 2)
    end as lower_band_20,
    sma_20,
    case
        when upper_band_20 is null then 0
        else round(CAST(upper_band_20 as numeric), 2)
    end as upper_band_20,
    case
        when lower_band_50 is null then 0
        else round(CAST(lower_band_50 as numeric), 2)
    end as lower_band_50,
    sma_50,
    case
        when upper_band_50 is null then 0
        else round(CAST(upper_band_50 as numeric), 2)
    end as upper_band_50,
    case
        when lower_band_100 is null then 0
        else round(CAST(lower_band_100 as numeric), 2)
    end as lower_band_100,
    sma_100,
    case
        when upper_band_100 is null then 0
        else round(CAST(upper_band_100 as numeric), 2)
    end as upper_band_100,
    case
        when lower_band_200 is null then 0
        else round(CAST(lower_band_200 as numeric), 2)
    end as lower_band_200,
    sma_200,
    case
        when upper_band_200 is null then 0
        else round(CAST(upper_band_200 as numeric), 2)
    end as upper_band_200,
    trading_date
from
    bollinger
ORDER BY
    ticker,
    trading_date desc