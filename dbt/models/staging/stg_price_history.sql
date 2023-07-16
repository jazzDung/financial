select 
    ticker,
    open,
    high,
    low,
    close,
    volume,
    to_timestamp(tradingDate, 'YYYY-MM-DDTHH:MI:SS.MS') as trading_date
    
from {{ source('sources', 'price_history') }}
