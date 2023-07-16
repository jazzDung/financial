
  
    

  create  table "financial_data"."marts"."dim_balance_sheet__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_balance_sheet"
  );
  