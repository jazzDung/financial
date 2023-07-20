from dagster import define_asset_job, AssetSelection
from financial.assets.run_info import *

ingest_all_job = define_asset_job(
    name="INGEST_EVERYTHING_EVERYWHERE_ALL_AT_ONCE",
    description="Ingest every available data",
    selection=AssetSelection.all()
)

send_email_job = define_asset_job(
    name="SEND_EMAIL_FOR_UNCHECKED_QUERY", 
    description="Check for record that have field value equal False every 30 seconds, then send email to the email in those records",
    selection=AssetSelection.keys("check_record")
        .upstream()
        .required_multi_asset_neighbors()
)

ingest_stock_history_job = define_asset_job(
    name="INGEST_STOCK_HISTORY", 
    description="Ingest stock price history, this job run daily",
    selection= AssetSelection.keys("marts/dim_price_history")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_cash_flow_job = define_asset_job(
    name="INGEST_CASH_FLOW", 
    description="Ingest organization cash flow reports, this job run at the start every quarter",
    selection= AssetSelection.keys("marts/dim_cash_flow")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_balance_sheet_job = define_asset_job(
    name="INGEST_BALANCE_SHEET", 
    description="Ingest organization balance sheet reports, this job run at the start every quarter",
    selection= AssetSelection.keys("marts/dim_balance_sheet")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_income_statement_job = define_asset_job(
    name="INGEST_INCOME_STATEMENT", 
    description="Ingest organization income statement reports, this job run at the start every quarter",
    selection= AssetSelection.keys("marts/dim_income_statement")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_general_rating_job = define_asset_job(
    name="INGEST_GENERAL_RATING", 
    description="Ingest organization general rating",
    selection= AssetSelection.keys("marts/dim_general_rating")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_organization_job = define_asset_job(
    name="INGEST_ORGANIZATION_OVERVIEW", 
    description="Ingest organization overview information, this job run at the start every quarterr",
    selection= AssetSelection.keys("marts/dim_organization")
        .upstream()
        .required_multi_asset_neighbors()
    )

create_model_job = define_asset_job(
    name="CREATE_USER_QUERY", 
    description="Create user query",
    selection=AssetSelection.keys("create_model")
        .upstream()
        .required_multi_asset_neighbors()
    )


