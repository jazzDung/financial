{% test relationships(model, column_name, field, to) %}

    {{ config(severity = 'warn') }}


with parent as (

    select
        {{ field }} as id

    from {{ to }}

),

child as (

    select
        {{ column_name }} as id

    from {{ model }}

)

select *
from child
where id is not null
  and id not in (select id from parent)

{% endtest %}