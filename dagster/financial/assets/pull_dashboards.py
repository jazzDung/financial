# dashboard exposures
import json
import logging
import argparse
from pathlib import Path

from dagster import asset
import ruamel.yaml
from financial.utils import (
    SupersetDBTConnectorSession,
    YamlFormatted,
    get_dashboards_from_superset,
    get_datasets_from_superset_dbt_refs,
    get_exposures_dict,
    get_tables_from_dbt,
    merge_dashboards_with_datasets,
)
from financial.resources import (
    DATABASE_ID,
    DBT_PROJECT_DIR,
    EXPOSURES_PATH,
    MANIFEST_PATH,
    SQL_DIALECT,
    SUPERSET_ADMIN_ID,
)
from dbt.cli.main import dbtRunner

# logging.basicConfig(level=logging.INFO)
logging.getLogger("sqlfluff").setLevel(level=logging.WARNING)
logger = logging.getLogger("__name__")


def _parse_args(argv):
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--user_id", help="""The id of user that create dashboards.""", required=False, default="reporting"
    )
    return parser.parse_args(argv)


@asset(group_name="dashboard")
def pull_dashboard():
    superset = SupersetDBTConnectorSession(logger=logger)

    logging.info("Starting the script!")

    dbt = dbtRunner()
    cli_args = [
        "parse",
        "--project-dir",
        DBT_PROJECT_DIR,
    ]
    res = dbt.invoke(cli_args)

    # TODO RECREATE DBT MANIFEST TO PARSE correctly

    with open(MANIFEST_PATH) as f:
        dbt_manifest = json.load(f)

    try:
        with open(EXPOSURES_PATH) as f:
            yaml = ruamel.yaml.YAML(typ="safe")
            exposures = yaml.load(f)["exposures"]

    except (FileNotFoundError, TypeError):
        Path(EXPOSURES_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(EXPOSURES_PATH).touch(exist_ok=True)
        exposures = {}

    dbt_tables = get_tables_from_dbt(dbt_manifest, None)

    dashboards, dashboards_datasets = get_dashboards_from_superset(superset, DATABASE_ID, SUPERSET_ADMIN_ID)

    datasets = get_datasets_from_superset_dbt_refs(superset, dashboards_datasets, dbt_tables, SQL_DIALECT, DATABASE_ID)

    dashboards = merge_dashboards_with_datasets(dashboards, datasets)

    exposures_dict = get_exposures_dict(dashboards, exposures)

    # insert empty line before each exposure, except the first
    exposures_yaml = ruamel.yaml.comments.CommentedSeq(exposures_dict)  # type: ignore
    for e in range(len(exposures_yaml)):
        if e != 0:
            exposures_yaml.yaml_set_comment_before_after_key(e, before="\n")

    exposures_yaml_schema = {"version": 2, "exposures": exposures_yaml}

    exposures_yaml_file = YamlFormatted()

    with open(EXPOSURES_PATH, "w+", encoding="utf-8") as f:
        exposures_yaml_file.dump(exposures_yaml_schema, f)

    print("Transferred into a YAML file at ", EXPOSURES_PATH)
    logging.info("All done!")
