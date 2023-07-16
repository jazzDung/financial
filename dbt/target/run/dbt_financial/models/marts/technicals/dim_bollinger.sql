
  
    

  create  table "financial_data"."marts"."dim_bollinger__dbt_tmp"
  
  
    as
  
  (
    select * from "financial_data"."intermediate"."int_bollinger"
  );
  