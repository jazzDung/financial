
  create view "financial_data"."staging"."stg_valuation_rating__dbt_tmp"
    
    
  as (
    select distinct on (ticker) 
	industryEn as industry_en,
	ticker as ticker,
	valuation as valuation,
	pe as pe,
	pb as pb,
	ps as ps,
	evebitda as evebitda,
	dividendRate as dividend_rate,
	_airbyte_emitted_at::date as index_date
from
	"financial_data"."sources"."valuation_rating"
where 
    ticker is not null
  );