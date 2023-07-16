
      update "financial_data"."snapshots"."orders_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "orders_snapshot__dbt_tmp153550008895" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "financial_data"."snapshots"."orders_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "financial_data"."snapshots"."orders_snapshot".dbt_valid_to is null;

    insert into "financial_data"."snapshots"."orders_snapshot" ("highestprice", "ticker", "lowestprice", "businessoperation", "businessmodel", "stockrating", "pricechange3m", "financialhealth", "rsrating", "valuation", "alpha", "tascore", "pricechange1y", "beta", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_general_rating_hashid", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."highestprice",DBT_INTERNAL_SOURCE."ticker",DBT_INTERNAL_SOURCE."lowestprice",DBT_INTERNAL_SOURCE."businessoperation",DBT_INTERNAL_SOURCE."businessmodel",DBT_INTERNAL_SOURCE."stockrating",DBT_INTERNAL_SOURCE."pricechange3m",DBT_INTERNAL_SOURCE."financialhealth",DBT_INTERNAL_SOURCE."rsrating",DBT_INTERNAL_SOURCE."valuation",DBT_INTERNAL_SOURCE."alpha",DBT_INTERNAL_SOURCE."tascore",DBT_INTERNAL_SOURCE."pricechange1y",DBT_INTERNAL_SOURCE."beta",DBT_INTERNAL_SOURCE."_airbyte_ab_id",DBT_INTERNAL_SOURCE."_airbyte_emitted_at",DBT_INTERNAL_SOURCE."_airbyte_normalized_at",DBT_INTERNAL_SOURCE."_airbyte_general_rating_hashid",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "orders_snapshot__dbt_tmp153550008895" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  