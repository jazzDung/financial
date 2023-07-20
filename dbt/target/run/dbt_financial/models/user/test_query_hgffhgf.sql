
  
    

  create  table "financial_data"."financial_user"."test_query_hgffhgf__dbt_tmp"
  
  
    as
  
  (
    
    -- depends_on: "financial_data"."marts"."dim_price_history"
WITH
original_query as (
    SELECT * from marts.dim_price_history
)
    
select * from original_query
  );
  