from dagster import Definitions
from financial.assets.pull_dashboards import *
from financial.assets.pull_user_description import *
from financial.assets.push_description import *
from financial.assets.dataset_sync import *
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
        [dataset_sync] +
        [pull_dashboards] +
        [pull_user_description] +
        [push_description]
        # [is_valid_table_name, create_dbt_model, get_records, update_records, create_model]
    ),
    
    jobs=[
        materialize_all_job, 
        ingest_price_history_job, 
        ingest_stock_intraday_job,
        ingest_organization_job,
        ingest_cash_flow_job,
        ingest_balance_sheet_job,
        ingest_income_statement_job,
        ingest_general_rating_job,
        ingest_business_model_rating_job,
        ingest_business_operation_rating_job,
        ingest_financial_health_rating_job,
        ingest_industry_health_rating_job,
        ingest_valuation_rating_job,
        calculate_bollinger_job,
        calculate_mfi_job,
        calculate_bov_job

        # create_model_job,
        # pull_dashboards_job,
        # pull_user_description_job,
        # push_description_job
    ],
        
    sensors=[
        check_new_records
    ],

    schedules=schedules,
)