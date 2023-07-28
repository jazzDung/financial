
  
    

  create  table "financial_data"."marts"."fact_financial_health_rating__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_financial_health_rating"
  );
  