import datetime
import json
import logging
import os
from itertools import compress

import sqlfluff
from dagster import asset

from dbt.cli.main import dbtRunner, dbtRunnerResult
from financial.resources import (
    DATABASE_ID,
    EMAIL_PASSWORD,
    EMAIL_PORT,
    EMAIL_SENDER,
    MODEL_TEMPLATE,
    SERVING_SCHEMA,
    SMTP,
    SUPERSET_ID,
    USER_MODEL_PATH,
    USER_SCHEMA,
    MANIFEST_PATH,
    DBT_PROJECT_DIR,
)
from pgsanity.pgsanity import check_string
from financial.utils import (
    SupersetDBTSessionConnector,
    get_emails,
    get_mail_content,
    get_physical_datasets_from_superset,
    get_records,
    get_ref,
    get_tables_from_dbt,
    is_unique_table_name,
    is_valid_table_name,
    update_records,
)


@asset(group_name="user_query")
def create_model():
    df, succeeded = get_records()

    if df.empty:
        logging.info("Early stopping because no records")
        return "Early stopping because no records"

    for filename in os.listdir(USER_MODEL_PATH):
        # If file is not present in list
        if filename.rstrip(".sql") not in succeeded or filename == "schema.yml":
            # Get full path of file and remove it
            full_file_path = os.path.join(USER_MODEL_PATH, filename)
            if os.path.isfile(full_file_path):
                os.remove(full_file_path)

    dbt = dbtRunner()
    cli_args = [
        "parse",
        "--project-dir",
        DBT_PROJECT_DIR,
    ]
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # Get dagster execution time, see: https://stackoverflow.com/questions/75099470/getting-current-execution-date-in-a-task-or-asset-in-dagster
    EXEC_TIME = datetime.datetime.today().strftime("%d/%m/%Y_%H:%M:%S")
    # raise Exception(DBT_PROJECT_DIR, MANIFEST_PATH, USER_MODEL_PATH)
    # Get all schema names in project
    # Either this or defined schema name available to the user before
    with open('target/manifest.json') as f:
        dbt_manifest = json.load(f)
        dbt_tables = get_tables_from_dbt(dbt_manifest, None)

    # Getting the dbt tables keys
    dbt_tables_names = list(dbt_tables.keys())
    mapped = map(lambda x: x.startswith((SERVING_SCHEMA, USER_SCHEMA)), dbt_tables_names)
    mask = list(mapped)

    dbt_tables_reporting = list(compress(dbt_tables_names, mask))

    dbt_names_aliases = [dbt_tables[table]["name"] for table in dbt_tables] + [
        dbt_tables[table]["alias"] for table in dbt_tables
    ]  # Name and aliases wo schema

    status = {}  # Status of preliminary checking
    for i in df.index:

        name_unique = is_unique_table_name(df.loc[i]["name"], dbt_names_aliases)  # check aliases and name
        if not name_unique:
            status[i] = "Model name is duplicated with another existing model"
            df.loc[i, "success"] = False
            continue

        # Check tables and add model ref
        ref_tables, processed_status = get_ref(df.loc[i, "query_string"], dbt_tables, dbt_tables_reporting)
        
        if processed_status != "Success":
            df.loc[i, "success"] = False
            status[i] = processed_status
            continue
        model_path = USER_MODEL_PATH + "/{name}.sql".format(name=df.loc[i, "name"])
        
        if os.path.exists(model_path):
            status[i] = "Model name is duplicated with another in processing batch"
            df.loc[i, "success"] = False
            continue

        with open(model_path, "w+") as f:
            template_output = MODEL_TEMPLATE.render(
                user_id=str(df.loc[i, "user_id"]),
                exec_time=EXEC_TIME,
                materialized='view',
                schema=USER_SCHEMA,
                refs=ref_tables,
                query=df.loc[i, "query_string"],
            )
            f.write(template_output)
            logging.info("Wrote model {name} contents".format(name=df.loc[i, "name"]))
            f.close()
        status[i] = "Success"
    # Get Emails from API
    superset = SupersetDBTSessionConnector()
    users = set(df["user_id"].to_list())
    email_list = get_emails(superset, users)
    email_dict = {key: value for element_dict in email_list for key, value in element_dict.items()}

    SMTP.login(EMAIL_SENDER, EMAIL_PASSWORD)

    for i in df.index:
        # Check Success
        if df.loc[i, "success"] == False:
            message = get_mail_content(df.loc[i, "name"], df.loc[i, "query_string"], status[i])
            # Add checked
            df.loc[i, "checked"] = True
            SMTP.sendmail(EMAIL_SENDER, email_dict[str(df.loc[i, "user_id"])], message)

    # If every record is unsuccesful, terminate script early
    if df['success'].all() == False and df['success'].isnull().sum() == 0:
        update_records(df)
        logging.info("Early stopping because no successful records")
        return "Early stopping because no successful records"


    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = [
        "run",
        "--project-dir",
        DBT_PROJECT_DIR,
        "--select",
        "tag:{exec_time}".format(exec_time=EXEC_TIME),
    ]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        logging.info(f"dbt run result: {r.node.name}: {r.status}")
    # Map df index to result
    dbt_res_df_map = {}

    for i in df.index:
        for r in res.result:
            if r.node.name == df.loc[i, "name"]:
                dbt_res_df_map[i] = r
                break

    sst_datasets = get_physical_datasets_from_superset(superset, DATABASE_ID)
    sst_user_tables = [(table["name"],table["id"]) for table in sst_datasets if table["schema"] == USER_SCHEMA and table["name"] in df[df.success!=False]["name"].values]
    sst_user_tables_name = {name:id for name,id in sst_user_tables}
    for i in df.index:
        # Check Success
        if df.loc[i, "success"] is not False:
            if dbt_res_df_map[i].status == "success":
                df.loc[i, "success"] = True

                rison_request = "/dataset/"

                if df.loc[i, "name"] in sst_user_tables_name.keys():
                    dataset_id = sst_user_tables_name[df.loc[i, "name"]]
                    response = superset.request("DELETE", f"/dataset/{dataset_id}")
                # Data to be written
                dictionary = {
                    # Parameter database
                    "database": DATABASE_ID,
                    "schema": USER_SCHEMA,
                    "table_name": df.loc[i, "name"],
                }

                
                # Serializing json
                response = superset.request("POST", rison_request, json=dictionary)
                
                body = {"description": df.loc[i,"description"], "owners": [int(df.loc[i, "user_id"]), SUPERSET_ID]}
                dataset_id = response["id"]
                response = superset.request("PUT", f"/dataset/{dataset_id}", json=body)

                message = get_mail_content(df.loc[i, "name"], df.loc[i, "query_string"], "dbt success")

            else:
                df.loc[i, "success"] = False
                message = get_mail_content(
                    df.loc[i, "name"], df.loc[i, "query_string"], "dbt fail", dbt_res_df_map[i].message
                )
            # Add checked
            df.loc[i, "checked"] = True

            SMTP.sendmail(EMAIL_SENDER, email_dict[str(df.loc[i, "user_id"])], message)

    SMTP.quit()
    # Delete unsucessful model
    for i in df.index:
        # Check Success
        if not df.loc[i, "success"]:
            full_file_path = os.path.join(USER_MODEL_PATH, "{name}.sql".format(name=df.loc[i, "name"]))
            if os.path.isfile(full_file_path):
                os.remove(full_file_path)

    update_records(df)
