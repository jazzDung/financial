
    
with dim_price_history as (
    select * from "financial_data"."marts"."dim_price_history"
    ),
    
original_query as (
    SELECT * from marts.dim_price_history
)
    
select * from original_query