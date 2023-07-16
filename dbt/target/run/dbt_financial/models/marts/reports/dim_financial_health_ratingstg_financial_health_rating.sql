
  
    

  create  table "financial_data"."financial_clean"."dim_financial_health_ratingstg_financial_health_rating__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_financial_health_rating"
  );
  