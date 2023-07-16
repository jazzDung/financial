
    
    

with child as (
    select ticker as from_field
    from "financial_data"."financial_raw"."stock_intraday"
    where ticker is not null
),

parent as (
    select ticker as to_field
    from "financial_data"."financial_clean"."dim_organization"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


