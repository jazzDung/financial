import os
from dagster import file_relative_path, with_resources
from financial.resources import DBT_PROFILE_PATH, DBT_PROJECT_DIR, DBT_TARGET
from dagster_dbt import DbtCliClientResource, load_assets_from_dbt_project

# # DBT_PROJECT_PATH = "/financial/dbt"
# DBT_PROJECT_PATH = "/home/jazzdung/projects/financial/dbt"

# DBT_PROJECT_DIR = (
#     DBT_PROJECT_PATH
#     if os.path.isabs(DBT_PROJECT_PATH)
#     else file_relative_path(__file__, DBT_PROJECT_PATH)
# )

# DBT_PROFILE_PATH = "/home/jazzdung/.dbt"
# DBT_TARGET = DBT_PROJECT_PATH + "/target/"
# # DBT_TARGET = "/financial/dbt/target/"

# # DBT_PROJECT_PATH = "/home/jazzdung/projects/financial/dbt"
# # DBT_PROJECT_DIR = DBT_PROJECT_PATH if os.path.isabs(DBT_PROJECT_PATH) else file_relative_path(__file__, DBT_PROJECT_PATH)
# # DBT_PROFILE_PATH = "/home/jazzdung/projects/financial/.dbt"
# # DBT_TARGET = "/home/jazzdung/projects/financial/dbt/target/"

dbt_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILE_PATH,
        target_dir=DBT_TARGET,
        display_raw_sql=True,
    ),
    {
        "dbt": DbtCliClientResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILE_PATH,
        ),
    },
)
