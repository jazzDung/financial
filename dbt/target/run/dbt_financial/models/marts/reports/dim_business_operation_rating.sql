
  
    

  create  table "financial_data"."financial_clean"."dim_business_operation_rating__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_business_operation_rating"
  );
  