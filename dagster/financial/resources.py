import json, sqlalchemy, smtplib, ssl, os
from jinja2 import Template

ENV = os.environ["FINANCIAL_ENVIROMENT"] if "FINANCIAL_ENVIROMENT" in os.environ else "dev"

PATH = "financial/dagster/financial/secret/" if ENV == "prod" else "financial/secret/"

# Get email info
f = open(PATH + "email.json")
data = json.load(f)[ENV]
EMAIL_SENDER = data["sender_email"]
EMAIL_PASSWORD = data["password"]
SMTP = data["smtp"]
EMAIL_PORT = data["port"]
f.close()

# Email authorization
context = ssl.create_default_context()
SMTP = smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context)

# Get database info
f = open(PATH + "postgres.json")
data = json.load(f)[ENV]
DATABASE_URL = data["url"]
USER_SCHEMA = data["user_schema"]
SERVING_SCHEMA = data["serving_schema"]
SQL_DIALECT = data["type"]
DATABASE_USERNAME = data["username"]
DATABASE_PASSWORD = data["password"]
DATABASE_HOST = data["host"]
DATABASE_PORT = data["port"]
DATABASE_NAME = data["db"]
QUERY_SCHEMA = data["query_schema"]
QUERY_TABLE = data["query_table"]
f.close()

# Setup connection
engine = sqlalchemy.create_engine(DATABASE_URL)
DB_CONNECTION = engine.connect()

# Airbyte connection
f = open(PATH + "airbyte.json")
data = json.load(f)[ENV]
AIRBYTE_HOST = data["host"]
AIRBYTE_PORT = data["port"]
AIRBYTE_USERNAME = data["username"]
AIRBYTE_PASSWORD = data["password"]
AIRBYTE_WORKSPACE = data["workspace"]
f.close()

# dbt connection
f = open(PATH + "dbt.json")
data = json.load(f)[ENV]
DBT_PROJECT_DIR = data["project"]
DBT_PROFILE_PATH = data["profile"]
DBT_TARGET = data["target"]
MANIFEST_PATH = data["target"] + "/manifest.json"
USER_MODEL_PATH = data["project"] + "/models/user"
EXPOSURES_PATH = USER_MODEL_PATH + "/exposures/dashboards.yml"
DESC_YAML_PATH = USER_MODEL_PATH + "/user_schema.yml"
f.close()


# Superset
f = open(PATH + "superset.json")
data = json.load(f)[ENV]
SUPERSET_ID = data["bot_id"]
SUPERSET_USERNAME = data["username"]
SUPERSET_PASSWORD = data["password"]
SUPERSET_HOST = data["host"]
SUPERSET_PUBLIC_HOST = data["public_host"]
DATABASE_ID = data["db"]
SUPERSET_ADMIN_ID = data["admin_id"]
SST_DATABASE_NAME = data["db_name"]
f.close()

# Create Query
MATERIALIZATION_MAPPING = {1: "table", 2: "view", 3: "incremental", 4: "ephemereal"}

with open(DBT_PROJECT_DIR + "/create_model.txt", "r") as f:
    MODEL_TEMPLATE = Template(f.read())
