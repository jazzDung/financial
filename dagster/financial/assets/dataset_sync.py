# sync dataset and tables
# Parse libraries
import argparse

# Script libraries
import logging
import json
from itertools import compress
from pathlib import Path
from financial.superset_utils.utils import (
    SupersetDBTConnectorSession,
    get_physical_datasets_from_superset,
    get_tables_from_dbt,
)
from financial.superset_utils.config import DATABASE_ID, MANIFEST_PATH, SERVING_SCHEMA, SUPERSET_ID

logger = logging.getLogger(__name__)


def _parse_args(argv):
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema_prefix_name",
        help="""The name of the schema, prefixed to the table names under the schema.""",
        required=False,
        default="reporting",
    )
    parser.add_argument(
        "--owner_id", help="""The user id of the tables' owner.""", required=False, default=14, type=int
    )
    return parser.parse_args(argv)


# gather argument
# flags = _parse_args(sys.argv[1:])

# schema_prefix_names=flags.schema_prefix_name
# database_number=flags.database_number
# owner_id=flags.owner_id


superset = SupersetDBTConnectorSession(logger=logger)
superset_tables_dict_list = get_physical_datasets_from_superset(superset, DATABASE_ID)
superset_tables_id_dict = dict([(table["key"], table["id"]) for table in superset_tables_dict_list])
print("Starting!")

dbt_tables = {}

if Path(MANIFEST_PATH).is_file():
    with open(MANIFEST_PATH) as f:
        dbt_manifest = json.load(f)
    dbt_tables_temp = get_tables_from_dbt(dbt_manifest, None)
    dbt_tables = {**dbt_tables, **dbt_tables_temp}
else:
    raise Exception("No exposures found at path")

# Getting the dbt tables keys
dbt_tables_names = list(dbt_tables.keys())
print("Tables in manifest: {num}".format(num=len(dbt_tables_names)))
print()
superset_dict_keys = [i["key"] for i in superset_tables_dict_list]
# Only tables that start with a given schema prefix name
mapped = map(lambda x: x.startswith(SERVING_SCHEMA), dbt_tables_names)
mask = list(mapped)
mapped_superset = map(lambda x: x.startswith(SERVING_SCHEMA), superset_dict_keys)
mask_superset = list(mapped_superset)

dbt_tables_reporting = list(compress(dbt_tables_names, mask))
superset_tables = list(compress(superset_dict_keys, mask_superset))


# Parsing as sets
dbt_tables_reporting = set(dbt_tables_reporting)
superset_tables = set(superset_tables)

# To add to superset
add_to_superset = list(dbt_tables_reporting.difference(superset_tables))
len(add_to_superset)

# To remove from superset

remove_from_superset = list(superset_tables.difference(dbt_tables_reporting))
len(remove_from_superset)

for i in add_to_superset:
    print("Starting datasets addition")
    print(i)
    rison_request = "/dataset/"
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
    if dbt_tables[add_to_superset]["user"]:
        dictionary["owners"].insert(dbt_tables[add_to_superset]["user"])
    # Serializing json
    json_object = json.dumps(dictionary)
    response = superset.request("POST", rison_request, json=dictionary)
print("Done!")
print("Starting superset datasets removal")
for i in remove_from_superset:
    # Dataset id to be deleted
    dataset_id = superset_tables_id_dict[i]

    rison_request = "/dataset/" + str(dataset_id)
    response = superset.request("DELETE", rison_request)
print("Done with removing tables!")
