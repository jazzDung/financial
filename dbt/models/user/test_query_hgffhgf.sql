
{{ config(
    materialized='table',
    name='test_query_hgffhgf',
    description='test description',
    tags = ['1','user_created','20/07/2023_16:58:56'],
    schema = 'financial_user'
) }}
    -- depends_on: {{ref('dim_price_history')}}
        ),
        
original_query as (
    SELECT * from marts.dim_price_history
)
    
select * from original_query
    