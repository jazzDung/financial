
      
  
    

  create  table "financial_data"."snapshots"."financial_health_rating_snapshot"
  
  
    as
  
  (
    

    select *,
        md5(coalesce(cast(ticker as varchar ), '')
         || '|' || coalesce(cast(_airbyte_emitted_at as varchar ), '')
        ) as dbt_scd_id,
        _airbyte_emitted_at as dbt_updated_at,
        _airbyte_emitted_at as dbt_valid_from,
        nullif(_airbyte_emitted_at, _airbyte_emitted_at) as dbt_valid_to
    from (
        



select * from "financial_data"."financial_raw"."financial_health_rating"

    ) sbq



  );
  
  