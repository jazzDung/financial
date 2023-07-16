
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "financial_data"."sources"."organization"
where ticker is not null
group by ticker
having count(*) > 1


