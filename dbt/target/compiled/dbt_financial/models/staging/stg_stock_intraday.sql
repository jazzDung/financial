select distinct on (ticker, t, id) 
	p as price,
	id,
	v as volume,
	cp as cp,
	rcp as rcp,
	a as a,
	ba as ba,
	sa as sa,
	hl as hl,
	pcp as pcp,
	t::timestamp as transaction_time
from
	"financial_data"."sources"."stock_intraday"
where 
    ticker is not null