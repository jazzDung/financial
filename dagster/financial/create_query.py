
import psycopg2
import json
import pandas as pd
from itertools import compress
import datetime
from pgsanity.pgsanity import check_string
import sqlparse
import re
import requests
from bs4 import BeautifulSoup
import smtplib, ssl
# from SupersetDBTConnector import SupersetDBTConnector
# from dataset_sync import get_tables_from_dbt
# from connector.SupersetDBTConnector import SupersetDBTConnector   
import os
from dbt.cli.main import dbtRunner, dbtRunnerResult
# from connector.pull_dashboards import get_tables_from_sql_simple
import logging

# Get dagster execution time, see: https://stackoverflow.com/questions/75099470/getting-current-execution-date-in-a-task-or-asset-in-dagster
EXEC_TIME = datetime.datetime.today().strftime("%d/%m/%Y_%H:%M:%S")

PROJECT_PATH = "/home/jazzdung/projects/financial/dbt/"
# MANIFEST_PATH = os.getenv('DBT_PROJECT_PATH')+"/target/manifest.json"
MANIFEST_PATH = PROJECT_PATH + "target/manifest.json"



MATERIALIZATION_MAPPING = {
    1:'table',
    2:'view',
    3:'incremental',
    4:'ephemereal'
}



def get_tables_from_sql_simple(sql):
    '''
    (Superset) Fallback SQL parsing using regular expressions to get tables names.
    '''
    sql = re.sub(r'(--.*)|(#.*)', '', sql)  
    sql = re.sub(r'\s+', ' ', sql).lower()  
    sql = re.sub(r'(/\*(.|\n)*\*/)', '', sql)  

    regex = re.compile(r'\b(from|join)\b\s+(\"?(\w+)\"?(\.))?\"?(\w+)\"?\b')  
    tables_match = regex.findall(sql)
    tables = [table[2] + '.' + table[4] if table[2] != '' else table[4]  
              for table in tables_match
              if table[4] != 'unnest'] 

    tables = list(set(tables))

    return tables
    
def is_valid_table_name(table_name):
  """
  Checks if the given string is a valid table name in PostgreSQL.

  Args:
    table_name: The string to check.

  Returns:
    True if the string is a valid table name, False otherwise.
  """

  # The regular expression to match a valid table name.
  regex = re.compile(r'^[a-zA-Z0-9_]{1,63}$')

  # Check if the string matches the regular expression.
  if regex.match(table_name):
    return True
  else:
    return False

def get_tables_from_dbt(dbt_manifest):
    tables = {}
    for table_type in ['nodes']: #removing sources from this list
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table['name'] #used for referencing within the dbt project
            schema = table['schema']
            database = table['database']
            alias = table['alias'] #used for referencing from the outside
            source = table['unique_id'].split('.')[-2]
            table_key = schema + '.' + alias
            tables[table_key] = {
                'name': name,
                'schema': schema,
                'database': database,
                'type': table_type[:-1],
                'ref':
                    f"ref('{name}')" if table_type == 'nodes'
                    else f"source('{source}', '{name}')"
            }
    
    return tables

# Get all schema names in project
# Either this or defined schema name available to the user before
with open('target/manifest.json') as f:
    dbt_manifest = json.load(f)
    dbt_tables=get_tables_from_dbt(dbt_manifest)

SCHEMA_NAMES = tuple(set([dbt_tables[table]["schema"]
                    for table in dbt_tables.keys() if not dbt_tables[table]["schema"].endswith("_dbt_test__audit")]))
SCHEMA_NAMES_WITH_DOT = tuple([schema + "." for schema in SCHEMA_NAMES])

def create_dbt_model(df_row, dbt_tables):
    """
    Returns content of a user-created dbt model file.

    Args:
        df_row: Row of DataFrame taken from "query" table.
        dbt_tables: Set of tables name.

    Returns:
        String: the content of the dbt model.
    """
    
    original_query = df_row['query_string']
    original_query = original_query[:-1] if original_query[-1] == ";" else original_query
    # Access table names
    table_names = set(get_tables_from_sql_simple(original_query))

    # Wrap original query
    new_query = """
original_query as (
    {original_query}
)
    
select * from original_query
    """.format(original_query=original_query)

    # Put tables in subqueries
    final_tables = tuple(table_names.intersection(dbt_tables)) # Filter out 
    table_to_ref = {}
    
    for table in final_tables:
       if table.startswith(SCHEMA_NAMES): table_to_ref[table] = table[table.find(".")+1:]
       else: table_to_ref[table] = table
    print("1")
    print(table_to_ref)
    print("2")
    print(final_tables)
    new_query = """
with {table} as (
    select * from {{{{ref(\'{table}\')}}}}
    ),
    """.format(table=table_to_ref[final_tables[0]]) + new_query
    
    if len(final_tables)>1:
        for table in final_tables[1:]:
            new_query = """
        {table} as (
        select * from {{{{ref(\'{table}\')}}}}
        ),
        """.format(table=table_to_ref[table]) + new_query

    new_query = """
{{{{ config(
    materialized=\'{materialization}\',
    name='{name}',
    description='{desc}',
    tags = ['user_created','{created_time}'],
    schema = 'financial_user'
) }}}}
    """.format(materialization=MATERIALIZATION_MAPPING[df_row['materialization']], 
               name=df_row['name'],
               desc=df_row['description'],
               created_time=EXEC_TIME) + new_query
   
    # original_query = re.sub(r".", "_", original_query)

    return new_query

def get_records():
    # Query records
    try:
        connection = psycopg2.connect(user="fdp",
                                    password="fdp",
                                    host="34.82.185.252",
                                    port="30005",
                                    database="financial_data",
                                    )
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from financial_query.query"
        # postgreSQL_select_Query = """
        # SELECT *
        # FROM query
        # WHERE insert_time  > now() - interval '30 second';
        # """

        cursor.execute(postgreSQL_select_Query)
        query_columns=['query_string',
                        'materialization',
                        'user_id',
                        'description',
                        'insert_time',
                        'name',
                        'success',
                        'checked']

        df = pd.DataFrame(cursor.fetchall(), 
                    columns=query_columns)

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
        connection = psycopg2.connect(user="fdp",
                                    password="fdp",
                                    host="34.82.185.252",
                                    port="30005",
                                    database="financial_data",
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
    res = superset.request("POST", "/security/get_email",json={"users_ids":user_ids})
    return res["emails"]

logger = logging.getLogger(__name__)
# Get table names with and without schemas
dbt_tables_names=list(dbt_tables.keys())

mapped = map (lambda x: x.startswith(SCHEMA_NAMES_WITH_DOT), dbt_tables_names)
mask=list(mapped)

dbt_tables_reporting=list(compress(dbt_tables_names, mask))

dbt_tables_with_schemas = [table.removeprefix(schema) for table in dbt_tables_reporting for schema in SCHEMA_NAMES_WITH_DOT]
status = [] # Status of preliminary checking

df = get_records()

for i in df.index:
    # Check name
    print(df.loc[i]['name'])
    name_validation = is_valid_table_name(df.loc[i]['name'])
    print(name_validation)
    if not name_validation:
        status.append("Invalid name")
        df.loc[i,'success'] = False
        # df.loc[i,'checked'] = True
        continue
    # Check syntax
    query_string = df.loc[i]['query_string']
    query_string = query_string + ";" if query_string[-1] != ";" else query_string
    validation = check_string(query_string)
    if not validation[0]:
        df.loc[i,'success'] = False
        # df.loc[i,'checked'] = True
        status.append("Invalid query: {error}".format(error=validation[1]))
        continue
    # Check multi-query
    if len(sqlparse.split(query_string))>1:
        df.loc[i,'success'] = False
        # df.loc[i,'checked'] = True
        status.append("Multiple statement")
        continue
    if sqlparse.parse(query_string)[0].get_type() != "SELECT":
        df.loc[i,'success'] = False
        # df.loc[i,'checked'] = True
        status.append("Query is not 'SELECT'")
        continue
    model_path = PROJECT_PATH + "models/user/{name}.sql".format(name=df.loc[i,'name'])
     
    with open(model_path,"w+") as f:
        f.write(create_dbt_model(df.loc[i], dbt_tables_with_schemas))
        f.close()
    status.append("Success")
print(status)
# Get Emails from API
# superset = SupersetDBTConnector(logger=logger,refresh_token=True)
# users = set(df["user_id"].to_list())
# emails = get_emails(superset, users)
# email_dict = dict(zip(df.user_id.to_list(), emails))
email_dict = {1:"catvu113@gmail"}

import smtplib, ssl

port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "catvu113@gmail.com"  
password = 'xhtzakhmnsbufufy'

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    for i in df.index:
        # Check Success
        if  df.loc[i,"success"] == False:

            message = """\
    Subject: Superset Model Creation

    Your Model was unsuccessfully created.
    
    Reason:
    {reason}

    SQL:
    {sql}
    """.format(reason=status[i], sql=df.loc[i,'query_string'])

            df.loc[i,'checked'] = True
            server.login(sender_email, password)
            server.sendmail(sender_email, email_dict[df.loc[i,'user_id']], message)




# initialize
dbt = dbtRunner()

# create CLI args as a list of strings
cli_args = ["run", 
            "--project-dir",
            PROJECT_PATH,
            "--select", 
            # "tag:{exec_time}".format(exec_time=EXEC_TIME)
            "tag:user_created"
            ]

# run the command
res: dbtRunnerResult = dbt.invoke(cli_args)

# inspect the results
for r in res.result:
    print(f"{r.node.name}: {r.status}")
# Map df index to result
dbt_res_df_map={}

for i in df.index:
    for r in res.result:
        if r.node.name == df.loc[i,'name']: dbt_res_df_map[i]=r
        break
context = ssl.create_default_context()


with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    for i in df.index:
        # Check Success
        if df.loc[i,"success"] is None or df.loc[i,"success"] is True:
            if dbt_res_df_map[i].status=="success":
                message = """\
    Subject: Superset Model Creation

    Your Model was unsuccessfully created.

    SQL:{sql}
    """.format(sql=df.loc[i,'query_string'])
            else:
                message = """\
    Subject: Superset Model Creation

    Your Model was successfully created during dbt's run, please contact the administrator.
    
    Reason:
    {reason}

    SQL:
    {sql}
    """.format(reason=dbt_res_df_map[i].message, sql=df.loc[i,'query_string'])

            df.loc[i,'checked'] = True
            server.login(sender_email, password)
            server.sendmail(sender_email, email_dict[df.loc[i,'user_id']], message)

entries_to_update = str(tuple(zip(df.name,df.user_id,df.checked,df.success))).replace("None","Null")[1:-1]
update_records(entries_to_update)
