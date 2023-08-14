{{config(
materialized = 'view',
tags = ['1', '12/08/2023_10:02:03'],
schema = 'user'
)}} 

-- depends_on: {{ ref('dim_organization') }}

select * from marts.dim_organization