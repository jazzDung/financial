
  
    

  create  table "financial_data"."technical"."dim_mfi__dbt_tmp"
  
  
    as
  
  (
    select * from "financial_data"."intermediate"."int_mfi"
  );
  