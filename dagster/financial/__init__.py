from dagster import Definitions
from financial.assets.dataset_sync import dataset_sync
from financial.assets.create_query import *
from financial.assets.dbt_assets import *
from financial.assets.airbyte_assets import *
from financial.assets.test_email_sender import *
from financial.sensors import *
from financial.jobs import *
from financial.schedules import *

defs = Definitions(
    assets=(
        dbt_assets +
        [airbyte_assets] +
        [fetch_unchecked, send_email, check_record] +
        [create_model] +
        [dataset_sync]
        # [is_valid_table_name, create_dbt_model, get_records, update_records, create_model]
    ),
    
    jobs=[
        ingest_all_job, 
        send_email_job, 
        ingest_stock_history_job, 
        ingest_organization_job,
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