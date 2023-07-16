
  create view "financial_data"."staging"."stg_stock_intraday__dbt_tmp"
    
    
  as (
    select
	p as price,
	v as volume,
	cp as cp,
	rcp as rcp,
	a as a,
	ba as ba,
	sa as sa,
	hl as hl,
	pcp as pcp,
	t as transaction_time
from
	"financial_data"."financial_raw"."stock_intraday"
  );