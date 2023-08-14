{{config(
materialized = 'view',
tags = ['1', '12/08/2023_10:02:03'],
schema = 'user'
)}} 

-- depends_on: {{ ref('fact_price_history') }}
-- depends_on: {{ ref('user_created_example_1') }}

select * from marts.fact_price_history