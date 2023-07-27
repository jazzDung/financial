from dagster import AssetKey
from financial.resources import AIRBYTE_HOST, AIRBYTE_PASSWORD, AIRBYTE_PORT, AIRBYTE_USERNAME, AIRBYTE_WORKSPACE
from dagster_airbyte import airbyte_resource, load_assets_from_airbyte_instance

airbyte_instance = airbyte_resource.configured(
    {
        "host": AIRBYTE_HOST,
        "port": AIRBYTE_PORT,
        "username": AIRBYTE_USERNAME,
        "password": AIRBYTE_PASSWORD,
    }
)

airbyte_assets = load_assets_from_airbyte_instance(
    airbyte_instance, 
    
    workspace_id=AIRBYTE_WORKSPACE,
    connection_to_asset_key_fn=lambda c, n: AssetKey(["source", n]),
)