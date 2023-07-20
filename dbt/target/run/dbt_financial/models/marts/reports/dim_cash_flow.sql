
  
    

  create  table "financial_data"."marts"."dim_cash_flow__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_cash_flow"
  );
  