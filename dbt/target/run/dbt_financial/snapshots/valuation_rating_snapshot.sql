
      update "financial_data"."snapshots"."valuation_rating_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "valuation_rating_snapshot__dbt_tmp153550005726" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "financial_data"."snapshots"."valuation_rating_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "financial_data"."snapshots"."valuation_rating_snapshot".dbt_valid_to is null;

    insert into "financial_data"."snapshots"."valuation_rating_snapshot" ("pb", "ticker", "ps", "evebitda", "valuation", "pe", "dividendrate", "industryen", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_valuation_rating_hashid", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."pb",DBT_INTERNAL_SOURCE."ticker",DBT_INTERNAL_SOURCE."ps",DBT_INTERNAL_SOURCE."evebitda",DBT_INTERNAL_SOURCE."valuation",DBT_INTERNAL_SOURCE."pe",DBT_INTERNAL_SOURCE."dividendrate",DBT_INTERNAL_SOURCE."industryen",DBT_INTERNAL_SOURCE."_airbyte_ab_id",DBT_INTERNAL_SOURCE."_airbyte_emitted_at",DBT_INTERNAL_SOURCE."_airbyte_normalized_at",DBT_INTERNAL_SOURCE."_airbyte_valuation_rating_hashid",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "valuation_rating_snapshot__dbt_tmp153550005726" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  