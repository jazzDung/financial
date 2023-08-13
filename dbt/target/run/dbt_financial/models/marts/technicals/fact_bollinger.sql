
  
    

  create  table "financial_data"."marts"."fact_bollinger__dbt_tmp"
  
  
    as
  
  (
    select 
    ticker,
    lower_band_5,
    sma_5,
    upper_band_5,
    lower_band_10,
    sma_10,
    upper_band_10,
    lower_band_20,
    sma_20,
    upper_band_20,
    lower_band_50,
    sma_50,
    upper_band_50,
    lower_band_100,
    sma_100,
    upper_band_100,
    lower_band_200,
    sma_200,
    upper_band_200,
    trading_date
    from "financial_data"."intermediate"."int_bollinger"
  );
  