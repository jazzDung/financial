
  create view "financial_data"."financial_raw"."stg_industry_health_rating__dbt_tmp"
    
    
  as (
    select
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
	netDebtEBITDA as net_debt_ebitda
from
	"financial_data"."financial_raw"."industry_health_rating"
  );