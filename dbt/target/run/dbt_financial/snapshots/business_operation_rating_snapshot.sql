
      update "financial_data"."snapshots"."business_operation_rating_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "business_operation_rating_snapshot__dbt_tmp153549756946" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "financial_data"."snapshots"."business_operation_rating_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "financial_data"."snapshots"."business_operation_rating_snapshot".dbt_valid_to is null;

    insert into "financial_data"."snapshots"."business_operation_rating_snapshot" ("avgroe", "ticker", "businessoperation", "avgroa", "last5yearsoperatingprofitgrowth", "industryen", "netinterestincomegrowth", "lastyearnetprofitmargin", "lastyearoperatingprofitmargin", "lastyeargrossprofitmargin", "toigrowth", "netinterestmargin", "costtoincome", "netincometoi", "last5yearsrevenuegrowth", "last5yearsebitdagrowth", "depositgrowth", "loangrowth", "last5yearsfcffgrowth", "last5yearsnetprofitgrowth", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_business_operation_rating_hashid", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."avgroe",DBT_INTERNAL_SOURCE."ticker",DBT_INTERNAL_SOURCE."businessoperation",DBT_INTERNAL_SOURCE."avgroa",DBT_INTERNAL_SOURCE."last5yearsoperatingprofitgrowth",DBT_INTERNAL_SOURCE."industryen",DBT_INTERNAL_SOURCE."netinterestincomegrowth",DBT_INTERNAL_SOURCE."lastyearnetprofitmargin",DBT_INTERNAL_SOURCE."lastyearoperatingprofitmargin",DBT_INTERNAL_SOURCE."lastyeargrossprofitmargin",DBT_INTERNAL_SOURCE."toigrowth",DBT_INTERNAL_SOURCE."netinterestmargin",DBT_INTERNAL_SOURCE."costtoincome",DBT_INTERNAL_SOURCE."netincometoi",DBT_INTERNAL_SOURCE."last5yearsrevenuegrowth",DBT_INTERNAL_SOURCE."last5yearsebitdagrowth",DBT_INTERNAL_SOURCE."depositgrowth",DBT_INTERNAL_SOURCE."loangrowth",DBT_INTERNAL_SOURCE."last5yearsfcffgrowth",DBT_INTERNAL_SOURCE."last5yearsnetprofitgrowth",DBT_INTERNAL_SOURCE."_airbyte_ab_id",DBT_INTERNAL_SOURCE."_airbyte_emitted_at",DBT_INTERNAL_SOURCE."_airbyte_normalized_at",DBT_INTERNAL_SOURCE."_airbyte_business_operation_rating_hashid",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "business_operation_rating_snapshot__dbt_tmp153549756946" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  