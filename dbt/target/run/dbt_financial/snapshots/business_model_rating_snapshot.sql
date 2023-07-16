
      update "financial_data"."snapshots"."business_model_rating_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "business_model_rating_snapshot__dbt_tmp153549753578" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "financial_data"."snapshots"."business_model_rating_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "financial_data"."snapshots"."business_model_rating_snapshot".dbt_valid_to is null;

    insert into "financial_data"."snapshots"."business_model_rating_snapshot" ("bom", "ticker", "assetquality", "companyposition", "businessmodel", "businessadministration", "industry", "operationrisk", "businessadvantage", "businessefficiency", "cashflowquality", "productservice", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_business_model_rating_hashid", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."bom",DBT_INTERNAL_SOURCE."ticker",DBT_INTERNAL_SOURCE."assetquality",DBT_INTERNAL_SOURCE."companyposition",DBT_INTERNAL_SOURCE."businessmodel",DBT_INTERNAL_SOURCE."businessadministration",DBT_INTERNAL_SOURCE."industry",DBT_INTERNAL_SOURCE."operationrisk",DBT_INTERNAL_SOURCE."businessadvantage",DBT_INTERNAL_SOURCE."businessefficiency",DBT_INTERNAL_SOURCE."cashflowquality",DBT_INTERNAL_SOURCE."productservice",DBT_INTERNAL_SOURCE."_airbyte_ab_id",DBT_INTERNAL_SOURCE."_airbyte_emitted_at",DBT_INTERNAL_SOURCE."_airbyte_normalized_at",DBT_INTERNAL_SOURCE."_airbyte_business_model_rating_hashid",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "business_model_rating_snapshot__dbt_tmp153549753578" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  