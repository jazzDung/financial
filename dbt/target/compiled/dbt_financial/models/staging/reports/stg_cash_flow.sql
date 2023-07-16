select 
    ticker,
    quarter,
    year,
    investCost as invest_cost,
    fromInvest as from_invest,
    fromFinancial as from_financial,
    fromSale as from_sale,
    freeCashFlow as free_cash_flow

from "financial_data"."financial_raw"."cash_flow"