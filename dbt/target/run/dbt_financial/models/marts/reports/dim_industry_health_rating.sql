
  
    

  create  table "financial_data"."financial_clean"."dim_industry_health_rating__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_industry_health_rating"
  );
  