
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "financial_data"."marts"."dim_general_rating"
where ticker is not null
group by ticker
having count(*) > 1


