with test_data as (

    select
        19 as number_actual_columns,
        23 as value

)
select *
from test_data
where
    number_actual_columns != value