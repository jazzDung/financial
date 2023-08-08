
  create view "financial_data"."sources"."sda__dbt_tmp"
    
    
  as (
    select * from "financial_data"."staging"."stg_organization"
  );