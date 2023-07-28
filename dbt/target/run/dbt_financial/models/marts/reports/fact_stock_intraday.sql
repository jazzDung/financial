
  
    

  create  table "financial_data"."marts"."fact_stock_intraday__dbt_tmp"
  
  
    as
  
  (
    SELECT * FROM "financial_data"."staging"."stg_stock_intraday"
  );
  