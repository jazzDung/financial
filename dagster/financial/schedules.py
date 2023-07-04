from dagster import ScheduleDefinition
from financial.jobs import *

schedules=[

    ScheduleDefinition(
        job=ingest_stock_history_job,
        cron_schedule="@daily",
    ),

    ScheduleDefinition(
        job=ingest_org_overview_job,
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
    )

]