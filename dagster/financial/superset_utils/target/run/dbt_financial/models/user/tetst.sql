
  
    

  create  table "financial_data"."financial_user"."tetst__dbt_tmp"
  
  
    as
  
  (
    
    -- depends_on: "financial_data"."marts"."dim_price_history"
    SELECT * from marts.dim_price_history
  );
  