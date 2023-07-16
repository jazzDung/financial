
{{ config(
    materialized='table',
    name='test_query_hgffhgf',
    description='test description',
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
    