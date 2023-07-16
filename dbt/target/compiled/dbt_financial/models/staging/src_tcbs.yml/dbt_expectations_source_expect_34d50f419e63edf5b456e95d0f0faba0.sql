with test_data as (

    select
        18 as number_actual_columns,
        11 as value

)
select *
from test_data
where
    number_actual_columns != value