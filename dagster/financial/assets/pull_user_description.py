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
        {"name": table_dict["table_name"], "columns": table_dict["columns"]}
        for table_dict in result
        if table_dict["table_schema"] == USER_SCHEMA
    ]

    for table in result_list:
        table["columns"] = [column for column in table["columns"] if column["description"]]
        if not table["columns"]:
            table.pop("columns")

    tables_desc = {}
    for table in result:
        if table["table_schema"] == USER_SCHEMA:
            tables_desc[table["table_name"]] = table["table_desc"]

    desc_yaml_file = YamlFormatted()

    col_desc_yaml_schema = {"version": 2, "models": result_list}
    with open(DESC_YAML_PATH, "w+", encoding="utf-8") as f:
        desc_yaml_file.dump(col_desc_yaml_schema, f)

    # write to each model file
    for table in tables_desc:
        with open(USER_MODEL_PATH + "/{table}.sql".format(table=table), "r") as file:
            text = file.read()
            pattern = r"description='(.*?)'|description=''"
            replacement = "description='{table_desc}'".format(table_desc=tables_desc[table])
            new_text = re.sub(pattern, replacement, text, 1)

        # with open(USER_MODEL_PATH + "/{table}.sql".format(table=table), "w") as file:
        #     file.write(new_text)
