import os
from dagster import DagsterRunStatus, sensor, RunRequest, RunConfig, SkipReason
from financial.resources import DB_CONNECTION
from financial.jobs import *
from dagster.core.storage.pipeline_run import RunsFilter


# @sensor(job=send_email_job, minimum_interval_seconds=60)
# def unchecked_records_exist():
#     output = DB_CONNECTION.execute(
#         """
#             SELECT exists 
#                 (SELECT 1 
#                 FROM financial_clean.user_query 
#                 WHERE checked = False 
#                 LIMIT 1);
#         """)
#     if output.fetchall()[0][0]:
#         yield RunRequest(run_key=None, run_config={})
#     else:
#         yield SkipReason("Found 0 unchecked record")


@sensor(job=create_model_job, minimum_interval_seconds=60)
def check_new_records(context):

    # run_records = context.instance.get_run_records(
    #     RunsFilter(job_name="INGEST_STOCK_HISTORY", statuses=[DagsterRunStatus.STARTED])
    # )

    run_records = context.instance.get_run_records(
        RunsFilter(tags=['dbt'], statuses=[DagsterRunStatus.STARTED])
    )


    output = DB_CONNECTION.execute(
        """
            SELECT exists 
                (SELECT 1 
                FROM financial_query.query 
                WHERE checked = False 
                LIMIT 1);
        """)
    if output.fetchall()[0][0] and len(run_records) == 0:
        yield RunRequest(run_key=None, run_config={})
    elif len(run_records) != 0:
        yield SkipReason("Price history is running, try again!")
    else:
        yield SkipReason("Found 0 unchecked record")
