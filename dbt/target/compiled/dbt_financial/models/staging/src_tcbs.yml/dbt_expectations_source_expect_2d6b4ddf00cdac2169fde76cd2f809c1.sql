with test_data as (

    select
        16 as number_actual_columns,
        16 as value

)
select *
from test_data
where
    number_actual_columns != value