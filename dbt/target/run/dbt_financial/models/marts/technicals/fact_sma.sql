
  
    

  create  table "financial_data"."marts"."fact_sma__dbt_tmp"
  
  
    as
  
  (
    select 
    ticker,
    sma_5,
    sma_10,
    sma_20,
    sma_50,
    sma_100,
    sma_200,
    trading_date
from         
    "financial_data"."intermediate"."int_sma"
  );
  