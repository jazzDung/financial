
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "financial_data"."sources"."financial_health_rating"
where ticker is not null
group by ticker
having count(*) > 1


