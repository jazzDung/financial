from financial.resources import EMAIL_SENDER, EMAIL_PASSWORD, DB_CONNECTION, SMTP
from dagster import asset, load_assets_from_modules
import os

@asset(group_name="user_model")
def create_model():
    # os.system("/home/jazzdung/projects/financial/dagster/financial/create_query.py")
    return None