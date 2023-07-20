import os

# Superset bot ID, username and password
SUPERSET_ID = 1
SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD")
SUPERSET_HOST = "http://34.82.185.252:30007/"
# dbt project path
DBT_PROJECT_PATH = "/home/vu/Desktop/Projects/Thesis/dbt-project/thesis"
# dbt project's manifest path
MANIFEST_PATH = DBT_PROJECT_PATH + "/target/manifest.json"

# Dashboard exposures path
EXPOSURES_PATH = DBT_PROJECT_PATH + "/models/exposures/dashboards.yml"

# The id of the database in superset, found in superset metadatabase.
DATABASE_ID = 1

# Schema of models
USER_SCHEMA = "user"
SERVING_SCHEMA = "marts"
# sqlfluff dialect
SQL_DIALECT = "postgres"
# dbt connection info
DB_USERNAME = "fdp"
DB_PASSWORD = "fdp"
DB_HOST = "34.82.185.252"
DB_PORT = "30005"
DB_DB = "financial_data"

# DB
# f = open(PATH + "airbyte.json")
# data = json.load(f)[ENV]
# AIRBYTE_HOST = data["host"]
# AIRBYTE_PORT = data["port"]
# AIRBYTE_USERNAME = data["username"]
# AIRBYTE_PASSWORD = data["password"]
# AIRBYTE_WORKSPACE = data["workspace"]
# f.close()
