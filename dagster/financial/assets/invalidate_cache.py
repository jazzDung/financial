import json
import logging
from dagster import asset
from requests import HTTPError
from pathlib import Path
from itertools import compress
from financial.utils import (
    SupersetDBTSessionConnector,
    get_tables_from_dbt,
)
from financial.resources import DATABASE_ID, MANIFEST_PATH, SERVING_SCHEMA, USER_SCHEMA, SST_DATABASE_NAME


@asset(group_name="dashboard")
def invalidate_cache():
    logging.basicConfig(level=logging.INFO)

    superset = SupersetDBTSessionConnector()

    if Path(MANIFEST_PATH).is_file():
        with open(MANIFEST_PATH) as f:
            dbt_manifest = json.load(f)
    else:
        raise Exception("No manifest found at path")

    dbt_tables = get_tables_from_dbt(dbt_manifest, None)
    serving_dbt_models = [
        dbt_tables[table] for table in dbt_tables if dbt_tables[table]["schema"] in (SERVING_SCHEMA, USER_SCHEMA)
    ]
    datasources = []
    for model in serving_dbt_models:
        datasources.append(
            {
                "database_name": SST_DATABASE_NAME,
                "datasource_name": model["name"],
                "datasource_type": "table",
                "schema": model["schema"],
            }
        )
    datasources = {"datasources": datasources}
    superset.request("POST", "cachekey/invalidate", json=datasources)
