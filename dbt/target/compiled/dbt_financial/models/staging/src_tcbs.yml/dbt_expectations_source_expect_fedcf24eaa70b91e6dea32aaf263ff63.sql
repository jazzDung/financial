with test_data as (

    select
        28 as number_actual_columns,
        28 as value

)
select *
from test_data
where
    number_actual_columns != value