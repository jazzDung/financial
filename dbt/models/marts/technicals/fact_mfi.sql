select 
    ticker,
    money_ratio,
    mfi,
    trading_date
from {{ ref('int_mfi') }}