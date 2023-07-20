with previous_volume as (
    select
        *,
        LAG(volume, 1) OVER (
            partition by ticker
            ORDER BY
                ticker,
                trading_date
        ) as pre_volume,
        LAG(close, 1) OVER (
            partition by ticker
            ORDER BY
                ticker,
                trading_date
        ) as pre_close
    from
        "financial_data"."staging"."stg_price_history"
    order by
        ticker,
        trading_date desc
),
multiplier as (
    select
        *,
        case
            when close > pre_close then 1
            when close < pre_close then -1
            else 0
        end as bov_mul
    from
        previous_volume
),
bov as (
    select
        *,
        sum(pre_volume * bov_mul) OVER (
            PARTITION BY ticker
            ORDER by
                trading_date ROWS UNBOUNDED PRECEDING
        ) AS bov
    from
        multiplier
)
select
    ticker,
    open,
    high,
    low,
    close,
    volume,
    case
        when bov is null then 0
        else bov
    end as bov,
    trading_date
from
    bov
ORDER by
    ticker,
    trading_date desc