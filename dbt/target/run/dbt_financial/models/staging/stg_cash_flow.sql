
  create view "financial_data"."staging"."stg_cash_flow__dbt_tmp"
    
    
  as (
    select 
    ticker,
    quarter,
    year,
    investCost as invest_cost,
    fromInvest as from_invest,
    fromFinancial as from_financial,
    fromSale as from_sale,
    freeCashFlow as free_cash_flow

from "financial_data"."sources"."cash_flow"
  );