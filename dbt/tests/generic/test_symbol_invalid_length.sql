{% test symbol_invalid_length(model, column_name) %}

with validation as (

    select
        {{ column_name }} as test_field

    from {{ model }}

),

validation_errors as (

    select
        test_field
    from validation
    
    where LENGTH(test_field) != 3

)

select *
from validation_errors

{% endtest %}