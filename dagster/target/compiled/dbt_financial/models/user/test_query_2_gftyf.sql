
    
with dim_price_history as (
    select * from "financial_data"."financial_clean"."dim_price_history"
    ),
    
original_query as (
    SELECT * from financial_clean.dim_price_history
)
    
select * from original_query