




with validation_errors as (

    select
        ticker,
        count(*) as "n_records"
    from "financial_data"."financial_raw"."general_rating"
    where
        1=1
        and 
    not (
        ticker is null
        
    )


    
    group by
        ticker
    having count(*) > 1

)
select * from validation_errors

