
{{ config(
    materialized='view',
    name='test_query_2_gftyf',
    description='test_desc',
    tags = ['user_created','15/07/2023_18:22:20'],
    schema = 'financial_user'
) }}
    
with dim_price_history as (
    select * from {{ref('dim_price_history')}}
    ),
    
original_query as (
    SELECT * from financial_clean.dim_price_history
)
    
select * from original_query
    