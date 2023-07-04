select 
    ticker,
    website,
    outstandingshare as outstanding_share,
    companytype as company_type,
    deltainyear as delta_in_year,
    industry,
    stockrating as stock_rating,
    industryen as industry_en,
    noemployees as no_employees,
    deltainweek as delta_in_week,
    industryid as industry_id,
    industryidv2 as industry_id_v2,
    exchange,
    establishedyear as established_year,
    shortname as short_name_en,
    noshareholders as no_share_holders,
    issueshare as issue_share,
    deltainmonth as delta_in_month,
    foreignpercent as foreign_percent

from {{ source('organization', 'organization_overview') }}