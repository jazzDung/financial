
  
    

  create  table "financial_data"."marts"."fact_income_statement__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_income_statement"
  );
  