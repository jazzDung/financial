
  create view "financial_data"."staging"."stg_business_operation_rating__dbt_tmp"
    
    
  as (
    select
	industryEn as industry_en,
	loanGrowth as loan_growth,
	depositGrowth as deposit_growth,
	netInterestIncomeGrowth as net_interest_income_growth,
	netInterestMargin as net_interest_margin,
	costToIncome as cost_to_income,
	netIncomeTOI as net_income_toi,
	ticker as ticker,
	businessOperation as business_operation,
	avgROE as avg_roe,
	avgROA as avg_roa,
	last5yearsNetProfitGrowth as last_5_years_net_profit_growth,
	last5yearsRevenueGrowth as last_5_years_revenue_growth,
	last5yearsOperatingProfitGrowth as last_5_years_operating_profit_growth,
	last5yearsEBITDAGrowth as last_5_years_ebitda_growth,
	last5yearsFCFFGrowth as last_5_years_fcff_growth,
	lastYearGrossProfitMargin as last_year_gross_profit_margin,
	lastYearOperatingProfitMargin as last_year_operating_profit_margin,
	lastYearNetProfitMargin as last_year_net_profit_margin,
	TOIGrowth as toi_growth,
	_airbyte_emitted_at::date as index_date
from
	"financial_data"."sources"."business_operation_rating"
  );