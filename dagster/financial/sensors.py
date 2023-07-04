import os
from dagster import sensor, RunRequest, RunConfig, SkipReason
from financial.resources import DB_CONNECTION
from financial.jobs import ingest_all_job, send_email_job

@sensor(job=send_email_job, minimum_interval_seconds=60)
def unchecked_records_exist():
    output = DB_CONNECTION.execute(
        """
            SELECT exists 
                (SELECT 1 
                FROM financial_clean.user_query 
                WHERE checked = False 
                LIMIT 1);
        """)
    if output.fetchall()[0][0]:
        yield RunRequest(run_key=None, run_config={})
    else:
        yield SkipReason("Found 0 unchecked record")