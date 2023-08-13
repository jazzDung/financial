import os
from dagster import file_relative_path, with_resources
from financial.resources import DBT_PROFILE_PATH, DBT_PROJECT_DIR, DBT_TARGET
from dagster_dbt import DbtCliClientResource, load_assets_from_dbt_project

dbt_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILE_PATH,
        target_dir=DBT_TARGET,
        display_raw_sql=True,
        use_build_command=True
    ),
    {
        "dbt": DbtCliClientResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILE_PATH,
        ),
    },
)
