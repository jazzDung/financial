
  
    

  create  table "financial_data"."financial_clean"."dim_cash_flow__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_cash_flow"
  );
  