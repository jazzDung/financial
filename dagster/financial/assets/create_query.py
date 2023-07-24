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
    SupersetDBTConnectorSession,
    add_materialization,
    get_emails,
    get_records,
    get_ref,
    get_tables_from_dbt,
    is_unique_table_name,
    is_valid_table_name,
    update_records,
)


@asset(group_name="user_query")
def create_model():
    logger = logging.getLogger("create_query")
    df = get_records()

    dbt = dbtRunner()
    cli_args = [
        "parse",
        "--project-dir",
        DBT_PROJECT_DIR,
    ]

    if df.empty:
        logging.info("Early stopping because no records")
        return "Early stopping because no records"

    # Get dagster execution time, see: https://stackoverflow.com/questions/75099470/getting-current-execution-date-in-a-task-or-asset-in-dagster
    EXEC_TIME = datetime.datetime.today().strftime("%d/%m/%Y_%H:%M:%S")
    # raise Exception(DBT_PROJECT_DIR, MANIFEST_PATH, USER_MODEL_PATH)
    # Get all schema names in project
    # Either this or defined schema name available to the user before
    with open(MANIFEST_PATH) as f:
        dbt_manifest = json.load(f)
        dbt_tables = get_tables_from_dbt(dbt_manifest, None)

    # Getting the dbt tables keys
    dbt_tables_names = set(list(dbt_tables.keys()))
    status = []  # Status of preliminary checking
    dbt_names_aliases = [dbt_tables[table]["name"] for table in dbt_tables] + [
        dbt_tables[table]["alias"] for table in dbt_tables
    ]  # Name and aliases wo schema

    for i in df.index:
        # Check name validity
        name_validation = is_valid_table_name(df.loc[i]["name"])
        if not name_validation:
            status.append("Invalid name")
            df.loc[i, "success"] = False
            continue
        name_unique = is_unique_table_name(df.loc[i]["name"], dbt_names_aliases)  # check aliases and name
        if not name_unique:
            status.append("Model name is duplicated with another existing model")
            df.loc[i, "success"] = False
            continue
        # Check syntax
        query_string = df.loc[i]["query_string"]
        query_string = query_string + ";" if query_string[-1] != ";" else query_string
        validation = check_string(query_string)
        if not validation[0]:
            df.loc[i, "success"] = False
            status.append("Invalid query: {error}".format(error=validation[1]))
            continue
        # Check multi-query
        parsed = sqlfluff.parse(query_string, "postgres")["file"]
        if type(parsed) == list:
            df.loc[i, "success"] = False
            status.append("Multiple statement")
            continue
        # Check select statements
        # if list(statement_list[0]["statement"].keys())[0] != "select_statement":
        #     df.loc[i, "success"] = False
        #     status.append("Query is not 'SELECT'")
        #     continue
        # Check tables and add model ref
        partially_model, processed_status = get_ref(df.loc[i, "query_string"], dbt_tables, parsed, dbt_tables_names)
        if processed_status != "Success":
            df.loc[i, "success"] = False
            status.append(processed_status)
            continue
        # Add config
        processed_model = add_materialization(df.loc[i], partially_model, EXEC_TIME)

        model_path = USER_MODEL_PATH + "/{name}.sql".format(name=df.loc[i, "name"])

        with open(model_path, "w+") as f:
            f.write(processed_model)
            logging.info("Wrote model {name} contents".format(name=df.loc[i, "name"]))
            f.close()
        status.append("Success")

    # Get Emails from API
    superset = SupersetDBTConnectorSession(logger=logger)
    users = set(df["user_id"].to_list())
    email_dict = get_emails(superset, users)

    SMTP.login(EMAIL_SENDER, EMAIL_PASSWORD)

    for i in df.index:
        # Check Success
        if df.loc[i, "success"] == False:
            message = """\
    Subject: Superset Model Creation

    Your Model was unsuccessfully created.

    Reason:
    {reason}

    SQL:
    {sql}
    """.format(
                reason=status[i], sql=df.loc[i, "query_string"]
            )

            df.loc[i, "checked"] = True
            SMTP.sendmail(EMAIL_SENDER, email_dict[str(df.loc[i, "user_id"])], message)

    # If every record is unsuccesful, terminate script early
    if not df["success"].any():
        entries_to_update = str(tuple(zip(df.name, df.user_id, df.checked, df.success))).replace("None", "Null")[1:-1]
        update_records(entries_to_update)
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

    for i in df.index:
        # Check Success
        if df.loc[i, "success"] is not False:
            if dbt_res_df_map[i].status == "success":
                df.loc[i, "success"] = True
                rison_request = "/dataset/"
                # Data to be written
                dictionary = {
                    # Parameter database
                    "database": DATABASE_ID,
                    "schema": USER_SCHEMA,
                    "table_name": df.loc[i, "name"],
                    "owners": [int(df.loc[i, "user_id"]), SUPERSET_ID],
                }
                # Serializing json
                json_object = json.dumps(dictionary)
                response = superset.request("POST", rison_request, json=dictionary)

                message = """\
    Subject: Superset Model Creation

    Your Model was successfully created. 

    SQL:{sql}
    """.format(
                    sql=df.loc[i, "query_string"]
                )

            else:
                df.loc[i, "success"] = False
                message = """\
    Subject: Superset Model Creation

    Your Model was unsuccessfully created during dbt's run, please contact the administrator.
    
    Reason:
    {reason}

    SQL:
    {sql}
    """.format(
                    reason=dbt_res_df_map[i].message, sql=df.loc[i, "query_string"]
                )

            df.loc[i, "checked"] = True

            SMTP.sendmail(EMAIL_SENDER, email_dict[str(df.loc[i, "user_id"])], message)

    # Delete unsucessful model
    for i in df.index:
        # Check Success
        if not df.loc[i, "success"]:
            model_path = "models/user/{name}.sql".format(name=df.loc[i, "name"])
            if os.path.exists(model_path):
                os.remove(model_path)

    entries_to_update = str(tuple(zip(df.name, df.user_id, df.checked, df.success))).replace("None", "Null")[1:-1]
    update_records(entries_to_update)
