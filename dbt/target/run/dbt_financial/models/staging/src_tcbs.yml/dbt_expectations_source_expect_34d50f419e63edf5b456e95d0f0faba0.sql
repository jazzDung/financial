select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with test_data as (

    select
        18 as number_actual_columns,
        11 as value

)
select *
from test_data
where
    number_actual_columns != value
      
    ) dbt_internal_test