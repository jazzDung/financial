
  
    

  create  table "financial_data"."marts"."dim_organization__dbt_tmp"
  
  
    as
  
  (
    select * from "financial_data"."staging"."stg_organization"
  );
  