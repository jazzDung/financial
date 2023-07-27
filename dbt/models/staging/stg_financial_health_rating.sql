select distinct on (ticker) 
	industryEn as industry_en,
	loanDeposit as loan_deposit,
	badLoanGrossLoan as bad_loan_gross_loan,
	badLoanAsset as bad_loan_asset,
	provisionBadLoan as provision_bad_loan,
	ticker as ticker,
	financialHealth as financial_health,
	netDebtEquity as net_debt_equity,
	currentRatio as current_ratio,
	quickRatio as quick_ratio,
	interestCoverage as interest_coverage,
	netDebtEBITDA as net_debt_ebitda,
	_airbyte_emitted_at::date as index_date
from
	{{ source('source', 'financial_health_rating') }}
where 
    ticker is not null