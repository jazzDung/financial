
  
    

  create  table "financial_data"."marts"."fact_industry_health_rating__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_industry_health_rating"
  );
  