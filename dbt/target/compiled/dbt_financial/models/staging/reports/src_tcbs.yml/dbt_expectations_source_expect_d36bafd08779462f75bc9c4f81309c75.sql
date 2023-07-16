with test_data as (

    select
        24 as number_actual_columns,
        24 as value

)
select *
from test_data
where
    number_actual_columns != value