
  
    

  create  table "financial_data"."financial_clean"."dim_price_history__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."financial_raw"."stg_price_history"
  );
  