with organization as (
    select * from {{ref ('stg_organization')}}
),

overview as (
    select * from {{ref('stg_organization_overview')}}
),

final as (
    select
        organization.ticker,
        organization.organ_name,
        organization.organ_short_name,
        overview.short_name_en,
        overview.exchange,
        organization.organ_type_code,
        organization.icb_code,
        organization.organ_code,
        organization.com_type_code,

        overview.website,
        overview.outstanding_share,
        overview.company_type,
        overview.delta_in_week,
        overview.delta_in_month,
        overview.delta_in_year,
        overview.industry,
        overview.industry_en,
        overview.stock_rating,
        overview.no_employees,
        overview.industry_id,
        overview.industry_id_v2,
        overview.established_year,
        overview.no_share_holders,
        overview.issue_share,
        overview.foreign_percent

    from organization join overview using (ticker)


)

select * from final