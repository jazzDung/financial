import re

from dagster import asset
from financial.utils import SupersetDBTSessionConnector, YamlFormatted
from financial.resources import DATABASE_ID, USER_MODEL_PATH, USER_SCHEMA, DESC_YAML_PATH


@asset(group_name="dashboard")
def pull_user_description():
    superset = SupersetDBTSessionConnector()

    res = superset.request("GET", f"/database/get_tables_descriptions/?db_id={DATABASE_ID}")

    result = res["result"]

    result_list = [
        {"name": table_dict["table_name"], "description": table_dict["table_desc"], "columns": table_dict["columns"]}
        for table_dict in result
        if table_dict["table_schema"] == USER_SCHEMA
    ]

    for table in result_list:
        table["columns"] = [column for column in table["columns"] if column["description"]]
        if not table["columns"]:
            table.pop("columns")

    desc_yaml_file = YamlFormatted()

    col_desc_yaml_schema = {"version": 2, "models": result_list}
    with open(DESC_YAML_PATH, "w+", encoding="utf-8") as f:
        desc_yaml_file.dump(col_desc_yaml_schema, f)