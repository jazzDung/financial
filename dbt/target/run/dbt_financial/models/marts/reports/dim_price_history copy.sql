
  
    

  create  table "financial_data"."marts"."dim_price_history copy__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_price_history"
  );
  