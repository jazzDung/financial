
{{ config(
    materialized='table',
    name='test_query_hgffhgf',
    description='newdesc',
    tags = ['1','user_created','20/07/2023_18:48:25'],
    schema = 'financial_user'
) }}
    -- depends_on: {{ref('dim_price_history')}}
WITH
original_query as (
    SELECT * from marts.dim_price_history
)
    
select * from original_query
    