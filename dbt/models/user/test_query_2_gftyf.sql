
{{ config(
    materialized='view',
    name='test_query_2_gftyf',
    description='test_desc',
    tags = ['1','user_created','20/07/2023_18:05:22'],
    schema = 'financial_user'
) }}
    -- depends_on: {{ref('dim_price_history')}}
WITH
original_query as (
    SELECT * from marts.dim_price_history
)
    
select * from original_query
    