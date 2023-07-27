select distinct on (ticker, index_date) 
    ticker,
    format('%s-%s-1', year, quarter)::date as index_date,
    investCost as invest_cost,
    fromInvest as from_invest,
    fromFinancial as from_financial,
    fromSale as from_sale,
    freeCashFlow as free_cash_flow

from {{ source('source', 'cash_flow') }}
where 
    ticker is not null