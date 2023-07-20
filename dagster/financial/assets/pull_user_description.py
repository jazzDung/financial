import logging
import re
from financial.superset_utils.utils import SupersetDBTConnectorSession, YamlFormatted
from financial.superset_utils.config import DATABASE_ID, USER_SCHEMA

logger = logging.getLogger(__name__)
superset = SupersetDBTConnectorSession(logger=logger)

res = superset.request("GET", f"/database/get_tables_descriptions/?q=[{DATABASE_ID}]")

result = res["result"]

result_list = [{"name": result[id]["table_name"], "columns": result[id]["columns"]} for id in result]

tables_desc = {}
for key in result.keys():
    if result[key]["table_schema"] == USER_SCHEMA:
        tables_desc[result[key]["table_name"]] = result[key]["table_desc"]

desc_yaml_file = YamlFormatted()

col_desc_yaml_schema = {"version": 2, "models": result_list}
desc_yaml_path = "/home/vu/Desktop/Projects/Thesis/dbt-project/thesis/models/user_schema.yml"
with open(desc_yaml_path, "w+", encoding="utf-8") as f:
    desc_yaml_file.dump(col_desc_yaml_schema, f)

# write to each model file
PROJECT_PATH = "/home/vu/Desktop/Projects/Thesis/financial/dbt"
for table in tables_desc:
    with open(PROJECT_PATH + "/models/user/{table}.sql".format(table=table), "r") as file:
        text = file.read()
        pattern = r"description='{my_desc}'"
        replacement = "description='{table_desc}'".format(table=table)
        new_text = re.sub(pattern, replacement, text)

    with open(PROJECT_PATH + "/models/{table}.sql".format(table=table), "w") as file:
        file.write(new_text)
