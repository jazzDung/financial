with test_data as (

    select
        41 as number_actual_columns,
        41 as value

)
select *
from test_data
where
    number_actual_columns != value