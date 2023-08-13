with price_history as (
	select
		*
	from
		{{ref('stg_price_history')}}
),
typical_price as (
	SELECT
		*,
		(high + low + close) / 3 as typical_price
	from
		price_history
),
yesterday_price as (
	select
		*,
		LAG(typical_price, -1) OVER (
			partition by ticker
			ORDER BY
				ticker,
				trading_date desc
		) as previous_price
	from
		typical_price
),
raw_flow as (
	select
		*,
		typical_price * volume as raw_flow
	from
		yesterday_price
),
flow as (
	select
		*,
		CASE
			WHEN typical_price > previous_price THEN 1
			WHEN typical_price < previous_price THEN 0
			ELSE NULL
		END as flow_type
	FROM
		raw_flow
),
flow_sum as (
	select
		*,
		SUM(COALESCE(raw_flow * flow_type, 0)) OVER(
			ORDER BY
				ticker,
				trading_date asc ROWS BETWEEN 14 PRECEDING
				AND CURRENT ROW
		) AS pos_sum,
		SUM(COALESCE(raw_flow * (1 - flow_type), 0)) OVER(
			ORDER BY
				ticker,
				trading_date asc ROWS BETWEEN 14 PRECEDING
				AND CURRENT ROW
		) AS neg_sum
	from
		flow
	ORDER BY
		ticker,
		trading_date desc
),
money_ratio as (
	select
		*,
		CASE
			WHEN neg_sum != 0
			AND pos_sum != 0 THEN pos_sum / neg_sum
			ELSE NULL
		END as money_ratio
	from
		flow_sum
),
mfi as (
	select
		ticker,
		open,
		high,
		low,
		close,
		volume,
		pos_sum :: bigint,
		neg_sum :: bigint,
		money_ratio :: real,
		Cast(100 - (100 /(1 + money_ratio)) as real) as mfi,
		trading_date
	from
		money_ratio
)
SELECT
	*
from
	mfi
ORDER BY
	ticker asc,
	trading_date desc 