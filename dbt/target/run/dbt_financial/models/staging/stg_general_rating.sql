
  create view "financial_data"."staging"."stg_general_rating__dbt_tmp"
    
    
  as (
    SELECT 
    stockRating AS stock_rating,
    valuation,
    financialHealth AS financial_health,
    businessModel AS business_model,
    businessOperation AS business_operation,
    rsRating AS rs_rating,
    taScore AS ta_score,
    ticker,
    highestPrice AS highest_price,
    lowestPrice AS lowest_price,
    priceChange3m AS price_change_3m,
    priceChange1y AS price_change_1y,
    beta,
    alpha,
	_airbyte_emitted_at::date as index_date
FROM "financial_data"."financial_raw"."general_rating"
  );