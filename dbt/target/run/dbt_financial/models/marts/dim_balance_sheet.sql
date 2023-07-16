
  
    

  create  table "financial_data"."financial_clean"."dim_balance_sheet__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."financial_raw"."stg_balance_sheet"
  );
  