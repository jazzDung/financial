
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "financial_data"."financial_clean"."dim_general_rating"
where ticker is not null
group by ticker
having count(*) > 1


