
  
    

  create  table "financial_data"."financial_clean"."dim_general_rating__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_general_rating"
  );
  