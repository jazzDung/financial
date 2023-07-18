
      update "financial_data"."snapshots"."industry_health_rating_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "industry_health_rating_snapshot__dbt_tmp184704412263" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "financial_data"."snapshots"."industry_health_rating_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "financial_data"."snapshots"."industry_health_rating_snapshot".dbt_valid_to is null;

    insert into "financial_data"."snapshots"."industry_health_rating_snapshot" ("ticker", "provisionbadloan", "quickratio", "badloanasset", "badloangrossloan", "interestcoverage", "netdebtebitda", "industryen", "loandeposit", "currentratio", "netdebtequity", "financialhealth", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_industry_health_rating_hashid", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."ticker",DBT_INTERNAL_SOURCE."provisionbadloan",DBT_INTERNAL_SOURCE."quickratio",DBT_INTERNAL_SOURCE."badloanasset",DBT_INTERNAL_SOURCE."badloangrossloan",DBT_INTERNAL_SOURCE."interestcoverage",DBT_INTERNAL_SOURCE."netdebtebitda",DBT_INTERNAL_SOURCE."industryen",DBT_INTERNAL_SOURCE."loandeposit",DBT_INTERNAL_SOURCE."currentratio",DBT_INTERNAL_SOURCE."netdebtequity",DBT_INTERNAL_SOURCE."financialhealth",DBT_INTERNAL_SOURCE."_airbyte_ab_id",DBT_INTERNAL_SOURCE."_airbyte_emitted_at",DBT_INTERNAL_SOURCE."_airbyte_normalized_at",DBT_INTERNAL_SOURCE."_airbyte_industry_health_rating_hashid",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "industry_health_rating_snapshot__dbt_tmp184704412263" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  