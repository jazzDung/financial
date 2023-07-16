
  
    

  create  table "financial_data"."financial_clean"."dim_income_statement__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."financial_raw"."stg_income_statement"
  );
  