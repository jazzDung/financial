select distinct on (ticker, index_date) 
    ticker,

    
    format('%s-%s-1', year, quarter)::date as index_date,
    shortAsset as short_asset,
    cash,
    shortInvest as short_invest,
    shortReceivable as short_receivable,
    inventory,
    longAsset as long_asset,
    fixedAsset as fixed_asset,
    asset,
    debt,
    shortDebt as short_debt,
    longDebt as long_debt,
    equity,
    capital,
    centralBankDeposit as central_bank_deposit,
    otherBankDeposit as other_bank_deposit,
    otherBankLoan as other_bank_loan,
    stockInvest as stock_invest,
    customerLoan as customer_loan,
    badLoan as bad_loan,
    provision,
    netCustomerLoan as net_customer_loan,
    otherAsset as other_asset,
    otherBankCredit as other_bank_credit,
    oweOtherBank as owe_other_bank,
    oweCentralBank as owe_central_bank,
    valuablePaper as valuable_paper,
    payableInterest as payable_interest,
    receivableInterest as receivable_interest,
    deposit,
    otherDebt as other_debt,
    fund,
    unDistributedIncome as un_distributed_income,
    minorShareHolderProfit as minor_share_holder_profit,
    payable
from
    {{source('source', 'balance_sheet')}}
where 
    ticker is not null
