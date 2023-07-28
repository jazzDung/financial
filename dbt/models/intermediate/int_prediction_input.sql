{{ 
  config(
    materialized = 'view',
    description = 'Prediction component input'
  ) 
}}

SELECT
  trading_date::date as trading_date,
  ticker,
  close as price
from
  {{ ref('fact_price_history') }}
WHERE
  trading_date BETWEEN CURRENT_DATE - INTERVAL '210' DAY
  AND CURRENT_DATE --fix the interval later, this is just for testing
ORDER BY
  ticker,
  trading_date