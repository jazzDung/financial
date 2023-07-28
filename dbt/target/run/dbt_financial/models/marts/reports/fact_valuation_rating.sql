
  
    

  create  table "financial_data"."marts"."fact_valuation_rating__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_valuation_rating"
  );
  