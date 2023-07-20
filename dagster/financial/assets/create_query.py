import datetime
import json

import logging
import re
import smtplib
import ssl
from itertools import compress

import pandas as pd
import psycopg2
import sqlfluff
import sqlparse
from dbt.cli.main import dbtRunner, dbtRunnerResult
from financial.resources import (
    DATABASE_ID,
    DB_DB,
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_USERNAME,
    SUPERSET_ID,
    USER_SCHEMA,
)
from pgsanity.pgsanity import check_string
from superset_utils.utils import (
    SupersetDBTConnectorSession,
    get_tables_from_dbt,
    get_tables_from_sql,
    get_tables_from_sql_simple,
)

def is_valid_table_name(table_name):
    """
    Checks if the given string is a valid table name in PostgreSQL.

    Args:
        table_name: The string to check.

    Returns:
        True if the string is a valid table name, False otherwise.
    """

    # The regular expression to match a valid table name.
    regex = re.compile(r"^[a-zA-Z0-9_]{1,63}$")

    # Check if the string matches the regular expression.
    if regex.match(table_name):
        return True
    else:
        return False


def create_dbt_model(df_row, dbt_tables):
    """
    Returns content of a user-created dbt model file.

    Args:
        df_row: Row of DataFrame taken from "query" table.
        dbt_tables: Set of tables name.

    Returns:
        String: the content of the dbt model.
    """

    original_query = df_row["query_string"]
    original_query = original_query[:-1] if original_query[-1] == ";" else original_query
    # Access table names
    try:
        original_query = sqlfluff.fix(original_query, dialect="posgres")
        table_names = set(get_tables_from_sql(original_query, dialect="posgres"))
    except:
        table_names = set(get_tables_from_sql_simple(original_query))

    # Wrap original query
    new_query = """
original_query as (
    {original_query}
)
    
select * from original_query
    """.format(
        original_query=original_query
    )

    # Put tables in subqueries
    final_tables = tuple(table_names.intersection(dbt_tables))  # Filter out
    assert len(final_tables) > 0, "No tables referenced in dbt projects"

    table_to_ref = {}

    for table in final_tables:
        if table.startswith(SCHEMA_NAMES):
            table_to_ref[table] = table[table.find(".") + 1 :]
        else:
            table_to_ref[table] = table
    for table in final_tables:
        new_query = (
            """
    -- depends_on: {{{{ref(\'{table}\')}}}}
        ),
        """.format(
                table=table_to_ref[final_tables[0]]
            )
            + new_query
        )

    new_query = (
        """
{{{{ config(
    materialized=\'{materialization}\',
    name='{name}',
    description='{desc}',
    tags = ['{user_id}','user_created','{created_time}'],
    schema = 'financial_user'
) }}}}""".format(
            materialization=MATERIALIZATION_MAPPING[df_row["materialization"]],
            user_id=df_row["user_id"],
            name=df_row["name"],
            desc=df_row["description"],
            created_time=EXEC_TIME,
        )
        + new_query
    )

    # original_query = re.sub(r".", "_", original_query)

    return new_query


def get_records():
    # Query records
    try:
        connection = psycopg2.connect(
            user=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DB,
        )
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from financial_query.query"
        # postgreSQL_select_Query = """
        # SELECT *
        # FROM query
        # WHERE insert_time  > now() - interval '30 second';
        # """

        cursor.execute(postgreSQL_select_Query)
        query_columns = [
            "query_string",
            "materialization",
            "user_id",
            "description",
            "insert_time",
            "name",
            "success",
            "checked",
        ]

        df = pd.DataFrame(cursor.fetchall(), columns=query_columns)

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    return df


def update_records(update_values):
    try:
        connection = psycopg2.connect(
            user=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DB,
        )
        cursor = connection.cursor()
        update_sql_query = f"""UPDATE financial_query.query q 
                                SET success = v.success,
                                    checked = v.checked

                                FROM (values {update_values}) AS v (name, user_id, checked, success)
                                WHERE q.user_id = v.user_id 
                                AND q.name = v.name;"""
        cursor.execute(update_sql_query)
    except (Exception, psycopg2.Error) as error:
        print("Error while updating data in PostgreSQL", error)

        cursor.execute(update_sql_query)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def get_emails(superset, user_ids):
    res = superset.request("POST", "/security/get_email", json={"users_ids": user_ids})
    return res["emails"]


def main():
    # Get dagster execution time, see: https://stackoverflow.com/questions/75099470/getting-current-execution-date-in-a-task-or-asset-in-dagster
    EXEC_TIME = datetime.datetime.today().strftime("%d/%m/%Y_%H:%M:%S")

    PROJECT_PATH = "/home/jazzdung/projects/financial/dbt/"
    # MANIFEST_PATH = os.getenv('DBT_PROJECT_PATH')+"/target/manifest.json"
    MANIFEST_PATH = PROJECT_PATH + "target/manifest.json"


    MATERIALIZATION_MAPPING = {1: "table", 2: "view", 3: "incremental", 4: "ephemereal"}


    # Get all schema names in project
    # Either this or defined schema name available to the user before
    with open(MANIFEST_PATH) as f:
        dbt_manifest = json.load(f)
        dbt_tables = get_tables_from_dbt(dbt_manifest, None)

    SCHEMA_NAMES = tuple(
        set(
            [
                dbt_tables[table]["schema"]
                for table in dbt_tables.keys()
                if not dbt_tables[table]["schema"].endswith("_dbt_test__audit")
            ]
        )
    )
    SCHEMA_NAMES_WITH_DOT = tuple([schema + "." for schema in SCHEMA_NAMES])


    logger = logging.getLogger(__name__)
    # Get table names with and without schemas
    dbt_tables_names = list(dbt_tables.keys())

    mapped = map(lambda x: x.startswith(SCHEMA_NAMES_WITH_DOT), dbt_tables_names)
    mask = list(mapped)

    dbt_tables_reporting = list(compress(dbt_tables_names, mask))

    dbt_tables_with_schemas = [
        table.removeprefix(schema) for table in dbt_tables_reporting for schema in SCHEMA_NAMES_WITH_DOT
    ]
    status = []  # Status of preliminary checking

    df = get_records()

    for i in df.index:
        # Check name
        print(df.loc[i]["name"])
        name_validation = is_valid_table_name(df.loc[i]["name"])
        print(name_validation)
        if not name_validation:
            status.append("Invalid name")
            df.loc[i, "success"] = False
            # df.loc[i,'checked'] = True
            continue
        # Check syntax
        query_string = df.loc[i]["query_string"]
        query_string = query_string + ";" if query_string[-1] != ";" else query_string
        validation = check_string(query_string)
        if not validation[0]:
            df.loc[i, "success"] = False
            # df.loc[i,'checked'] = True
            status.append("Invalid query: {error}".format(error=validation[1]))
            continue
        # Check multi-query
        if len(sqlparse.split(query_string)) > 1:
            df.loc[i, "success"] = False
            # df.loc[i,'checked'] = True
            status.append("Multiple statement")
            continue
        if sqlparse.parse(query_string)[0].get_type() != "SELECT":
            df.loc[i, "success"] = False
            # df.loc[i,'checked'] = True
            status.append("Query is not 'SELECT'")
            continue
        model_path = PROJECT_PATH + "models/user/{name}.sql".format(name=df.loc[i, "name"])

        with open(model_path, "w+") as f:
            model_file_content = create_dbt_model(df.loc[i], dbt_tables_with_schemas)
            if model_file_content:
                f.write(model_file_content)
                print("Wrote model {name} contents".format(name=df.loc[i, "name"]))
            else:
                df.loc[i, "success"] = False
                print("Model references no valid tables.".format(name=df.loc[i, "name"]))

            f.close()
        status.append("Success")
    print(status)
    # Get Emails from API
    superset = SupersetDBTConnectorSession(logger=logger)
    users = set(df["user_id"].to_list())
    emails = get_emails(superset, users)
    email_dict = dict(zip(df.user_id.to_list(), emails))

    import smtplib
    import ssl

    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "catvu113@gmail.com"
    password = "xhtzakhmnsbufufy"

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
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
                server.login(sender_email, password)
                server.sendmail(sender_email, email_dict[df.loc[i, "user_id"]], message)

    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = [
        "run",
        "--project-dir",
        PROJECT_PATH,
        "--select",
        # "tag:{exec_time}".format(exec_time=EXEC_TIME)
        "tag:user_created",
    ]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        print(f"{r.node.name}: {r.status}")
    # Map df index to result
    dbt_res_df_map = {}

    for i in df.index:
        for r in res.result:
            if r.node.name == df.loc[i, "name"]:
                dbt_res_df_map[i] = r
            break
    context = ssl.create_default_context()


    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        for i in df.index:
            # Check Success
            if df.loc[i, "success"] is None or df.loc[i, "success"] is True:
                if dbt_res_df_map[i].status == "success":
                    rison_request = "/dataset/"
                    # Data to be written
                    dictionary = {
                        # Parameter database
                        "database": DATABASE_ID,
                        "schema": USER_SCHEMA,
                        "table_name": df.loc[i, "name"],
                        "owners": [df.loc[i, "user_id"], SUPERSET_ID],
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
                server.login(sender_email, password)
                server.sendmail(sender_email, email_dict[df.loc[i, "user_id"]], message)

    entries_to_update = str(tuple(zip(df.name, df.user_id, df.checked, df.success))).replace("None", "Null")[1:-1]
    update_records(entries_to_update)
