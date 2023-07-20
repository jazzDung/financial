
    -- depends_on: "financial_data"."marts"."dim_price_history"
WITH
original_query as (
    SELECT * from dim_price_history
)
    
select * from original_query