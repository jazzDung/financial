select distinct on (ticker, index_date) 
    ticker,
    format('%s-%s-1', year, quarter)::date as index_date,
    revenue,
    yearRevenueGrowth as year_revenue_growth,
    quarterRevenueGrowth as quarter_revenue_growth,
    costOfGoodSold as cost_of_good_sold,
    grossProfit as gross_profit,
    operationExpense as operation_expense,
    operationProfit as operation_profit,
    yearOperationProfitGrowth as year_operation_profit_growth,
    quarterOperationProfitGrowth as quarter_operation_profit_growth,
    interestExpense as interest_expense,
    preTaxProfit as pre_tax_profit,
    postTaxProfit as post_tax_profit,
    shareHolderIncome as share_holder_income,
    yearShareHolderIncomeGrowth as year_share_holder_income_growth,
    quarterShareHolderIncomeGrowth as quarter_share_holder_income_growth,
    investProfit as invest_profit,
    serviceProfit as service_profit,
    otherProfit as other_profit,
    provisionExpense as provision_expense,
    operationIncome as operation_income,
    ebitda
from 
    {{source('source', 'income_statement')}}
where 
    ticker is not null