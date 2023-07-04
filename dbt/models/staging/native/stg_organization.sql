select 
    organname as organ_name,
    ticker,
    organtypecode as organ_type_code,
    icbcode as icb_code,
    organcode as organ_code,
    comtypecode as com_type_code,
    organshortname as organ_short_name,
    comgroupcode as com_group_code


from {{ source('organization', 'organization') }}