from dagster import Definitions
from .assets.dbt import *
from .assets.airbyte import *
from .assets.email import *
from .sensors import *
from .jobs import *
from .schedules import *

defs = Definitions(
    assets=(
        dbt_assets +
        [airbyte_assets] +
        [fetch_unchecked, send_email, check_record]
    ),
    
    jobs=[
        ingest_all_job, 
        send_email_job, 
        ingest_stock_history_job, 
        ingest_org_overview_job,
        ingest_cash_flow_job,
        ingest_balance_sheet_job,
        ingest_income_statement_job,
        ingest_general_rating_job,
        do_stuff
    ],
        
    sensors=[
        unchecked_records_exist
    ],

    schedules=schedules,
)