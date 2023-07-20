{{ config(materialized='view', description = 'Prediction component input') }}

SELECT
  "time_stamp" ,
  ticker,
  "close" as price
    from
        {{ ref('stg_price_history') }}
WHERE
  "time_stamp" BETWEEN CURRENT_DATE - INTERVAL '210' DAY AND CURRENT_DATE
--fix the interval later, this is just for testing
ORDER BY
  ticker,
  "time_stamp"