from dagster import ScheduleDefinition
from financial.jobs import *

schedules=[

    ScheduleDefinition(
        job=calculate_price_history_and_indicator,
        cron_schedule="0 0 * * 1,2,3,4,5",
    ),

    ScheduleDefinition(
        job=ingest_organization_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=ingest_cash_flow_job,
        cron_schedule="* * * 1-12/3 *"
    ),
    
    ScheduleDefinition(
        job=ingest_balance_sheet_job,
        cron_schedule="* * * 1-12/3 *"
    ), 

    ScheduleDefinition(
        job=ingest_income_statement_job,
        cron_schedule="* * * 1-12/3 *"
    ),   

    ScheduleDefinition(
        job=ingest_general_rating_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=ingest_stock_intraday_job,
        cron_schedule="*/15 * * * 1,2,3,4,5"
    ),

    ScheduleDefinition(
        job=ingest_business_model_rating_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=ingest_business_operation_rating_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=ingest_financial_health_rating_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=ingest_industry_health_rating_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=ingest_valuation_rating_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=calculate_bollinger_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=calculate_mfi_job,
        cron_schedule="* * * 1-12/3 *"
    ),

    ScheduleDefinition(
        job=calculate_bov_job,
        cron_schedule="* * * 1-12/3 *"
    )

]