
  
    

  create  table "financial_data"."marts"."fact_bov__dbt_tmp"
  
  
    as
  
  (
    select * from "financial_data"."intermediate"."int_bov"
  );
  