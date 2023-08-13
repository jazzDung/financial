
  
    

  create  table "financial_data"."marts"."fact_mfi__dbt_tmp"
  
  
    as
  
  (
    select 
    ticker,
    money_ratio,
    mfi,
    trading_date
from "financial_data"."intermediate"."int_mfi"
  );
  