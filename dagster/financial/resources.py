import json, sqlalchemy, smtplib, ssl, os

ENV = (
    os.environ["FINANCIAL_ENVIROMENT"]
    if "FINANCIAL_ENVIROMENT" in os.environ
    else "dev"
)

# Get email info 
f = open("financial/secret/email.json")
data = json.load(f)[ENV]
EMAIL_SENDER = data["sender_email"]
EMAIL_PASSWORD = data["password"]
f.close()

# Email authorization
context = ssl.create_default_context()
SMTP = smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context)

# Get database info
f = open("financial/secret/postgres.json")
data = json.load(f)[ENV]
DB_URL = data["url"]
f.close()

# Setup connection
engine = sqlalchemy.create_engine(DB_URL)
DB_CONNECTION = engine.connect()

# Airbyte connection
f = open("financial/secret/airbyte.json")
data = json.load(f)[ENV]
AIRBYTE_HOST = data["host"]
AIRBYTE_PORT = data["port"]
AIRBYTE_USERNAME = data["username"]
AIRBYTE_PASSWORD = data["password"]
AIRBYTE_WORKSPACE = data["workspace"]
f.close()

# dbt connection
f = open("financial/secret/dbt.json")
data = json.load(f)[ENV]
DBT_PROJECT_DIR = data["project"]
DBT_PROFILE_PATH = data["profile"]
DBT_TARGET = data["target"]
f.close()