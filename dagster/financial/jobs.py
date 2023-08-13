from dagster import define_asset_job, AssetSelection

materialize_all_job = define_asset_job(
    name="MATERIALIZE_EVERYTHING_EVERYWHERE_ALL_AT_ONCE",
    description="Materialize every available assets",
    selection=AssetSelection.all()
)

send_email_job = define_asset_job(
    name="SEND_EMAIL_FOR_UNCHECKED_QUERY", 
    description="Check for record that have field value equal False every 30 seconds, then send email to the email in those records",
    selection=AssetSelection.keys("check_record")
        .upstream()
        .required_multi_asset_neighbors()
)

ingest_organization_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_ORGANIZATION_OVERVIEW", 
    description="Ingest organization overview information, this job run at the start every quarterr",
    selection= AssetSelection.keys("marts/dim_organization")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_price_history_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_PRICE_HISTORY", 
    description="Ingest stock price history, this job run daily",
    selection= AssetSelection.keys("marts/fact_price_history")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_stock_intraday_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_STOCK_INTRADAY", 
    description="Ingest stock intraday transactions, this job run daily",
    selection= AssetSelection.keys("marts/fact_stock_intraday")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_cash_flow_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_CASH_FLOW", 
    description="Ingest organization cash flow reports, this job run at the start every quarter",
    selection= AssetSelection.keys("marts/fact_cash_flow")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_balance_sheet_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_BALANCE_SHEET", 
    description="Ingest organization balance sheet reports, this job run at the start every quarter",
    selection= AssetSelection.keys("marts/fact_balance_sheet")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_income_statement_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_INCOME_STATEMENT", 
    description="Ingest organization income statement reports, this job run at the start every quarter",
    selection= AssetSelection.keys("marts/fact_income_statement")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_general_rating_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_GENERAL_RATING", 
    description="Ingest organization general rating",
    selection= AssetSelection.keys("marts/fact_general_rating")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_business_model_rating_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_BUSINESS_MODEL_RATING", 
    description="Ingest organization business model rating",
    selection= AssetSelection.keys("marts/fact_business_model_rating")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_business_operation_rating_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_BUSINESS_OPERATION_RATING", 
    description="Ingest organization business operation rating",
    selection= AssetSelection.keys("marts/fact_business_operation_rating")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_financial_health_rating_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_FINANCIAL_HEALTH_RATING", 
    description="Ingest organization financial health rating",
    selection= AssetSelection.keys("marts/fact_financial_health_rating")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_industry_health_rating_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_INDUSTRY_HEALTH_RATING", 
    description="Ingest organization industry health rating",
    selection= AssetSelection.keys("marts/fact_industry_health_rating")
        .upstream()
        .required_multi_asset_neighbors()
    )

ingest_valuation_rating_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_VALUATION_RATING", 
    description="Ingest organization valuation rating",
    selection= AssetSelection.keys("marts/fact_valuation_rating")
        .upstream()
        .required_multi_asset_neighbors()
    )

calculate_bollinger_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_BOLLINGER", 
    description="Calculate symnol bollinger indicator",
    selection= AssetSelection.keys("marts/fact_bollinger")
        .upstream()
        .required_multi_asset_neighbors()
    )

calculate_mfi_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_MFI", 
    description="Calculate symnol mfi indicator",
    selection= AssetSelection.keys("marts/fact_mfi")
        .upstream()
        .required_multi_asset_neighbors()
    )

calculate_bov_job = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_BOV", 
    description="Calculate symnol bov indicator",
    selection= AssetSelection.keys("marts/fact_bov")
        .upstream()
        .required_multi_asset_neighbors()
    )

calculate_price_history_and_indicator = define_asset_job(
    tags={"airbyte": "True", "dbt": "True"},
    name="INGEST_PRICE_HISTORY_AND_INDICATOR", 
    description="Ingest price history and related indicator",
    selection= AssetSelection.keys("marts/fact_price_history", "marts/fact_mfi", "marts/fact_bov", "marts/fact_sma", "marts/fact_bollinger")
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

dataset_sync_job = define_asset_job(
    name="DATASET_SYNC", 
    description="Dataset sync",
    selection=AssetSelection.keys("dataset_sync")
        .upstream()
        .required_multi_asset_neighbors()
    )

pull_dashboards_job = define_asset_job(
    name="PULL_DASHBOARDS", 
    description="Pull dashboards",
    selection=AssetSelection.keys("pull_dashboards")
        .upstream()
        .required_multi_asset_neighbors()
    )

pull_user_description_job = define_asset_job(
    name="PULL_USER_DESCRIPTION", 
    description="Pull user description",
    selection=AssetSelection.keys("pull_user_description")
        .upstream()
        .required_multi_asset_neighbors()
    )

push_description_job = define_asset_job(
    name="PUSH_DESCRIPTION", 
    description="Push description",
    selection=AssetSelection.keys("push_description")
        .upstream()
        .required_multi_asset_neighbors()
    )

run_info_job = define_asset_job(
    name="RUN_INFO", 
    description="Run info",
    selection=AssetSelection.keys("run_info")
        .upstream()
        .required_multi_asset_neighbors()
    )



