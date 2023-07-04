from dagster import OpExecutionContext, op

@op
def get_run_id(context: OpExecutionContext):
    print(context.log.info(f"My run ID is {context.run_id}"))
