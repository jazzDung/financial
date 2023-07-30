# push desc
import json
import logging
from dagster import asset
from requests import HTTPError
from pathlib import Path

from financial.utils import (
    SupersetDBTSessionConnector,
    add_sst_dataset_metadata,
    add_superset_columns,
    get_physical_datasets_from_superset,
    get_tables_from_dbt,
    merge_columns_info,
    put_columns_to_superset,
    refresh_columns_in_superset,
)
from financial.resources import DATABASE_ID, MANIFEST_PATH, USER_SCHEMA


@asset(group_name="dashboard")
def push_description(dataset_sync):
    logging.basicConfig(level=logging.INFO)

    superset = SupersetDBTSessionConnector()

    logging.info("Starting the script!")

    sst_datasets = get_physical_datasets_from_superset(superset, DATABASE_ID)
    logging.info("There are %d physical datasets in Superset.", len(sst_datasets))


    
    if Path('target/manifest.json').is_file():
        with open('target/manifest.json') as f:
            dbt_manifest = json.load(f)
    else:
        raise Exception("No manifest found at path")

    dbt_tables = get_tables_from_dbt(dbt_manifest, None)

    for i, sst_dataset in enumerate(sst_datasets):
        columns_refreshed = 0
        logging.info("Processing dataset %d/%d.", i + 1, len(sst_datasets))
        sst_dataset_id = sst_dataset["id"]
        sst_dataset_key = sst_dataset["key"]
        if sst_dataset["schema"] == USER_SCHEMA:
            continue  # Don't push user description

        refresh_columns_in_superset(superset, sst_dataset_id)
        columns_refreshed = 1

        if columns_refreshed == 1:
            columns_refreshed = 1
        else:
            refresh_columns_in_superset(superset, sst_dataset_id)
        # Otherwise, just adding the normal analytics certification
        sst_dataset_w_cols = add_superset_columns(superset, sst_dataset)
        sst_dataset_w_cols_new = merge_columns_info(sst_dataset_w_cols, dbt_tables)
        put_columns_to_superset(superset, sst_dataset_w_cols_new)
        add_sst_dataset_metadata(superset, sst_dataset_id, sst_dataset_key, dbt_tables)

    logging.info("All done!")
