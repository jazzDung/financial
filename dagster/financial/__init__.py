from dagster import Definitions
from financial.assets.model import *
from financial.assets.dbt import *
from financial.assets.airbyte import *
from financial.assets.email import *
from financial.sensors import *
from financial.jobs import *
from financial.schedules import *

defs = Definitions(
    assets=(
        dbt_assets +
        [airbyte_assets] +
        [fetch_unchecked, send_email, check_record] +
        [create_model]
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
        create_model_job
    ],
        
    sensors=[
        check_new_records
    ],

    schedules=schedules,
)