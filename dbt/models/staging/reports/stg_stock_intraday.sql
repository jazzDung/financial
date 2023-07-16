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
	{{ source('raw', 'stock_intraday') }}