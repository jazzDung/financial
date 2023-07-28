
  
    

  create  table "financial_data"."marts"."fact_price_history__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_price_history"
  );
  