
  create view "financial_data"."staging"."stg_valuation_rating__dbt_tmp"
    
    
  as (
    select
	industryEn as industry_en,
	ticker as ticker,
	valuation as valuation,
	pe as pe,
	pb as pb,
	ps as ps,
	evebitda as evebitda,
	dividendRate as dividend_rate
from
	"financial_data"."financial_raw"."valuation_rating"
  );