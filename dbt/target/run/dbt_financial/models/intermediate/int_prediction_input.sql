
  create view "financial_data"."intermediate"."int_prediction_input__dbt_tmp"
    
    
  as (
    

SELECT
  trading_date::date as trading_date,
  ticker,
  close as price
from
  "financial_data"."marts"."dim_price_history"
WHERE
  trading_date BETWEEN CURRENT_DATE - INTERVAL '210' DAY
  AND CURRENT_DATE --fix the interval later, this is just for testing
ORDER BY
  ticker,
  trading_date
  );