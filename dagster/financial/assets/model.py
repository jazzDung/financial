from financial.resources import EMAIL_SENDER, EMAIL_PASSWORD, DB_CONNECTION, SMTP
from dagster import asset
from financial.create_query import *

@asset(group_name="user_model")
def create_model():
    main()