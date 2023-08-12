import logging
import json
from itertools import compress
from pathlib import Path
from dagster import asset
from financial.utils import (
    SupersetDBTSessionConnector,
    get_physical_datasets_from_superset,
    get_tables_from_dbt,
)
from financial.resources import DATABASE_ID, DBT_PROJECT_DIR, MANIFEST_PATH, SERVING_SCHEMA, USER_SCHEMA, SUPERSET_ID
from dbt.cli.main import dbtRunner


@asset(group_name="dashboard")
def dataset_sync():
    superset = SupersetDBTSessionConnector()
    superset_tables_dict_list = get_physical_datasets_from_superset(superset, DATABASE_ID)
    superset_tables_id_dict = dict([(table["key"], table["id"]) for table in superset_tables_dict_list])
    logging.info("Starting!")
    
    dbt = dbtRunner()
    cli_args = [
        "parse",
        "--project-dir",
        DBT_PROJECT_DIR,
    ]
    res = dbt.invoke(cli_args)
    
    dbt_tables = {}
    with open('target/manifest.json') as f:
        dbt_manifest = json.load(f)
    dbt_tables_temp = get_tables_from_dbt(dbt_manifest, None)
    dbt_tables = {**dbt_tables, **dbt_tables_temp}

        

    # Getting the dbt tables keys
    dbt_tables_names = list(dbt_tables.keys())
    logging.info("Tables in manifest: {num}".format(num=len(dbt_tables_names)))
    superset_dict_keys = [i["key"] for i in superset_tables_dict_list]
    # Only tables that start with a given schema prefix name
    mapped = map(lambda x: x.startswith((SERVING_SCHEMA, USER_SCHEMA)), dbt_tables_names)
    mask = list(mapped)

    dbt_tables_reporting = list(compress(dbt_tables_names, mask))
    superset_tables = superset_dict_keys

    # Parsing as sets
    dbt_tables_reporting = set(dbt_tables_reporting)
    superset_tables = set(superset_tables)

    # To add to superset
    add_to_superset = list(dbt_tables_reporting.difference(superset_tables))

    # To remove from superset

    remove_from_superset = list(superset_tables.difference(dbt_tables_reporting))

    for i in add_to_superset:
        logging.info("Starting datasets addition")
        rison_request = "dataset/"
        array = i.split(".")
        schema = array[0]
        table_name = array[1]
        # Data to be written
        dictionary = {
            # Parameter database
            "database": DATABASE_ID,
            "schema": schema,
            "table_name": array[1],
            "owners": [SUPERSET_ID],
        }
        # Add potential user
        if dbt_tables[i]["user"]:
            dictionary["owners"].append(dbt_tables[i]["user"])
        # Serializing json
        json_object = json.dumps(dictionary)
        try:
            response = superset.request("POST", rison_request, json=dictionary)
        except:
            raise Exception(dictionary)
    logging.info("Done!")
    logging.info("Starting superset datasets removal")
    for i in remove_from_superset:
        # Dataset id to be deleted
        dataset_id = superset_tables_id_dict[i]

        rison_request = "/dataset/" + str(dataset_id)
        response = superset.request("DELETE", rison_request)
    logging.info("Done with removing tables!")
