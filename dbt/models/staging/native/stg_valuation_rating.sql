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
	{{ source('financial_raw', 'valuation_rating') }}