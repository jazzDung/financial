
  create view "financial_data"."financial_user"."test_query_2_gftyf__dbt_tmp"
    
    
  as (
    
    
with dim_price_history as (
    select * from "financial_data"."financial_clean"."dim_price_history"
    ),
    
original_query as (
    SELECT * from financial_clean.dim_price_history
)
    
select * from original_query
  );