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
from financial.resources import DBT_PROJECT_DIR, MANIFEST_PATH, SERVING_SCHEMA, USER_SCHEMA, SST_DATABASE_NAME
from dbt.cli.main import dbtRunner

@asset(group_name="dashboard")
def invalidate_cache():
    logging.basicConfig(level=logging.INFO)

    dbt = dbtRunner()
    cli_args = [
        "parse",
        "--project-dir",
        DBT_PROJECT_DIR,
    ]
    res = dbt.invoke(cli_args)
    if not res.success:
        raise Exception("Unable to parse project.")
    
    superset = SupersetDBTSessionConnector()

    if Path('target/manifest.json').is_file():
        with open('target/manifest.json') as f:
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
