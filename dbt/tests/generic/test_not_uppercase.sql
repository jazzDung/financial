{% test not_uppercase(model, column_name) %}

    {{ config(severity = 'warn') }}

with validation as (

    select
        {{ column_name }} as test_field

    from {{ model }}

),

validation_errors as (

    select
        test_field
    from validation
    
    where test_field != UPPER(test_field)

)

select *
from validation_errors

{% endtest %}