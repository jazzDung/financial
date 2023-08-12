
  create view "financial_data"."user"."user_created_example__dbt_tmp"
    
    
  as (
     

-- depends_on: "financial_data"."marts"."fact_price_history"

select * from marts.fact_price_history
  );