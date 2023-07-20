import json, sqlalchemy, smtplib, ssl, os

ENV = os.environ["FINANCIAL_ENVIROMENT"] if "FINANCIAL_ENVIROMENT" in os.environ else "dev"

PATH = "financial/dagster/financial/secret/" if ENV == "prod" else "financial/secret/"

# Get email info
f = open(PATH + "email.json")
data = json.load(f)[ENV]
EMAIL_SENDER = data["sender_email"]
EMAIL_PASSWORD = data["password"]
f.close()

# Email authorization
context = ssl.create_default_context()
SMTP = smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context)

# Get database info
f = open(PATH + "postgres.json")
data = json.load(f)[ENV]
DB_URL = data["url"]
f.close()

# Setup connection
engine = sqlalchemy.create_engine(DB_URL)
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
f.close()


# Superset bot ID, username and password
SUPERSET_ID = 2
SUPERSET_USERNAME = "superset"
SUPERSET_PASSWORD = "superset"
SUPERSET_HOST = "http://34.82.185.252:30007/"
# dbt project path
DBT_PROJECT_DIR = "/home/jazzdung/projects/financial/dbt"
# dbt project's manifest path
MANIFEST_PATH = DBT_PROJECT_DIR + "/target/manifest.json"

# Dashboard exposures path
EXPOSURES_PATH = DBT_PROJECT_DIR + "/models/exposures/dashboards.yml"

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
