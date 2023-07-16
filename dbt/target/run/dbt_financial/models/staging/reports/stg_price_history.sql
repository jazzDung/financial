
  create view "financial_data"."staging"."stg_price_history__dbt_tmp"
    
    
  as (
    select 
    ticker,
    open,
    high,
    low,
    close,
    volume,
    to_timestamp(tradingDate, 'YYYY-MM-DDTHH:MI:SS.MS') as trading_date
    
from "financial_data"."financial_raw"."price_history"
  );