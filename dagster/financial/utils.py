import logging
import re
import sqlfluff
from bs4 import BeautifulSoup
import requests
from markdown import markdown
import ruamel.yaml

from superset_utils.config import SUPERSET_HOST, SUPERSET_PASSWORD, SUPERSET_USERNAME


#### ADD ALL OF THE END POINT USED TO CSRF EXEMPT LIST TO RUN PARALLELY
#### ONLY USE SESSION FOR SEQUENTIAL RUNNING SCRIPTS


class SupersetDBTConnectorSession:
    """A class for accessing the Superset API in an easy way."""

    def __init__(self, logger):
        """Instantiates the class.

        ''access_token'' will be instantiated via enviromental variable
        If ``access_token`` is None, attempts to obtain it using ``refresh_token``.

        Args:
            api_url: Base API URL of a Superset instance, e.g. https://my-superset/api/v1.
            access_token: Access token to use for accessing protected endpoints of the Superset
                API. Can be automatically obtained if ``refresh_token`` is not None.
            refresh_token: Refresh token to use for obtaining or refreshing the ``access_token``.
                If None, no refresh will be done.
        """
        self.url = SUPERSET_HOST
        self.api_url = self.url + "api/v1/"

        self.logger = logger
        self.session = requests.session()

        self.username = SUPERSET_USERNAME
        self.password = SUPERSET_PASSWORD
        self.headers = {}
        self._refresh_session()

    def _refresh_session(self):
        self.logger.info("Refreshing session")

        self.soup = BeautifulSoup(self.session.post(self.url + "/login").text, "html.parser")
        self.csrf_token = self.soup.find("input", {"id": "csrf_token"})["value"]  # type: ignore

        data = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True,
        }
        self.headers = {
            # 'Authorization': 'Bearer {}'.format(self.access_token),
            "x-csrftoken": self.csrf_token,
        }
        response = self.session.post("http://localhost:8088/login", json=data, headers=self.headers)  # type: ignore
        return True

    def request(self, method, endpoint, refresh_session_if_needed=True, headers=None, **request_kwargs):
        """Executes a request against the Superset API.

        Args:
            method: HTTP method to use.
            endpoint: Endpoint to use.
            refresh_token_if_needed: Whether the ``access_token`` should be automatically refreshed
                if needed.
            headers: Additional headers to use.
            **request_kwargs: Any ``requests.request`` arguments to use.

        Returns:
            A dictionary containing response body parsed from JSON.

        Raises:
            HTTPError: There is an HTTP error (detected by ``requests.Response.raise_for_status``)
                even after retrying with a fresh ``access_token``.
        """

        self.logger.info("About to %s execute request for endpoint %s", method, endpoint)

        if headers is None:
            headers = {}

        url = self.api_url + endpoint
        res = self.session.request(method, url, headers=self.headers, **request_kwargs)  # type: ignore

        self.logger.info("Request finished with status: %d", res.status_code)

        if (
            refresh_session_if_needed
            and res.status_code == 401
            and res.json().get("msg") == "Token has expired"
            and self._refresh_session()
        ):
            self.logger.info("Retrying %s request for endpoint %s with refreshed session")
            res = self.session.request(method, url, headers=self.headers, **request_kwargs)  # type: ignore

            self.logger.info("Request finished with status: %d", res.status_code)

        if (
            refresh_session_if_needed
            and res.status_code == 400
            and res.json()["message"] == "400 Bad Request: The CSRF session token is missing."
            and self._refresh_session()
        ):
            self.logger.info("Retrying %s request for endpoint %s with refreshed session")
            res = self.session.request(method, url, headers=self.headers, **request_kwargs)  # type: ignore
            self.logger.info("Request finished with status: %d", res.status_code)
        res.raise_for_status()
        return res.json()


def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    tables = {}
    for table_type in ["nodes"]:
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table["name"]
            schema = table["schema"]
            database = table["database"]
            alias = table["alias"]
            source = table["unique_id"].split(".")[-2]
            table_key = schema + "." + alias

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key not in tables, (
                    f"Table {table_key} is a duplicate name (schema + table) across databases. "
                    "This would result in incorrect matching between Superset and dbt. "
                    "To fix this, remove duplicates or add ``dbt_db_name``."
                )
                tables[table_key] = {
                    "name": name,
                    "schema": schema,
                    "database": database,
                    "type": table_type[:-1],
                    "ref": f"ref('{name}')" if table_type == "nodes" else f"source('{source}', '{name}')",
                    "user": None,
                }
            if schema == "user":
                tables[table_key]["user"] = table["tags"][0]

    assert tables, "Manifest is empty!"

    return tables


def get_physical_datasets_from_superset(superset: SupersetDBTConnectorSession, superset_db_id):
    logging.info("Getting physical datasets from Superset.")
    page_number = 0
    datasets = []
    datasets_keys = set()
    while True:
        logging.info("Getting page %d.", page_number + 1)
        rison_request = f"dataset/?q=(page_size:100,page:{page_number},order_column:changed_on_delta_humanized,order_direction:asc,filters:!((col:table_name,opr:nct,value:archived),(col:sql,opr:dataset_is_null_or_empty,value:true)))"
        res = superset.request("GET", rison_request)
        result = res["result"]
        if result:
            for r in result:
                name = r["table_name"]
                schema = r["schema"]
                database_name = r["database"]["database_name"]
                dataset_id = r["id"]
                database_id = r["database"]["id"]
                dataset_key = f"{schema}.{name}"  # same format as in dashboards

                kind = r["kind"]
                if kind == "physical" and (superset_db_id is None or database_id == superset_db_id):
                    dataset_id = r["id"]

                    name = r["table_name"]
                    schema = r["schema"]
                    dataset_key = f"{schema}.{name}"  # used as unique identifier

                    dataset_dict = {
                        "id": dataset_id,
                        "name": name,
                        "schema": schema,
                        "database": database_name,
                        "dataset_id": dataset_id,
                        "key": dataset_key,
                        "table": [dataset_key],
                    }

                    # fail if it breaks uniqueness constraint
                    assert dataset_key not in datasets_keys, (
                        f"Dataset {dataset_key} is a duplicate name (schema + table) "
                        "across databases. "
                        "This would result in incorrect matching between Superset and dbt. "
                        "To fix this, remove duplicates or add the ``superset_db_id`` argument."
                    )

                    datasets_keys.add(dataset_key)
                    datasets.append(dataset_dict)

            page_number += 1
        else:
            break

    return datasets


def get_tables_from_sql_simple(sql):
    """
    (Superset) Fallback SQL parsing using regular expressions to get tables names.
    """
    sql = re.sub(r"(--.*)|(#.*)", "", sql)
    sql = re.sub(r"\s+", " ", sql).lower()
    sql = re.sub(r"(/\*(.|\n)*\*/)", "", sql)

    regex = re.compile(r"\b(from|join)\b\s+(\"?(\w+)\"?(\.))?\"?(\w+)\"?\b")
    tables_match = regex.findall(sql)
    tables = [
        table[2] + "." + table[4] if table[2] != "" else table[4] for table in tables_match if table[4] != "unnest"
    ]

    tables = list(set(tables))

    return tables


def get_tables_from_sql(sql, dialect):
    """
    (Superset) SQL parsing using sqlfluff to get clean tables names.
    If sqlfluff parsing fails it runs the above regex parsing fun.
    Returns a tables list.
    """
    try:
        sql_parsed = sqlfluff.parse(sql, dialect=dialect)
        tables_raw = [table.raw for table in sql_parsed.tree.recursive_crawl("table_reference")]  # type: ignore
        tables_cleaned = [".".join(table.replace('"', "").lower().split(".")[-2:]) for table in tables_raw]
    except (
        sqlfluff.core.errors.SQLParseError,  # type: ignore
        sqlfluff.core.errors.SQLLexError,  # type: ignore
        sqlfluff.core.errors.SQLFluffUserError,  # type: ignore
        sqlfluff.api.simple.APIParsingError,  # type: ignore
    ) as e:  # type: ignore
        logging.warning(
            "Parsing SQL through sqlfluff failed. "
            "Let me attempt this via regular expressions at least and "
            "check the problematic query and error below.\n%s",
            sql,
            exc_info=e,
        )
        tables_cleaned = get_tables_from_sql_simple(sql)

    tables = list(set(tables_cleaned))

    return tables


def get_dashboards_from_superset(superset: SupersetDBTConnectorSession, superset_db_id, user_id):
    """
    This function gets
    1. Get dashboards id list from Superset iterating on the pages of the url
    2. Get a dashboard detail information :
        title, owner, url, unique datasets names

    Returns dashboards, dashboards_datasets
    """

    logging.info("Getting published dashboards from Superset.")
    page_number = 0
    dashboards_id = []
    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request("GET", f'/dashboard/?q={{"page":{page_number},"page_size":100}}')
        result = res["result"]
        if result:
            for r in result:
                if r["published"] and r["created_by"]["id"] == user_id:
                    dashboards_id.append(r["id"])
            page_number += 1
        else:
            break

    assert len(dashboards_id) > 0, "There are no dashboards in Superset!"

    logging.info("There are %d published dashboards in Superset.", len(dashboards_id))

    dashboards = []
    dashboards_datasets_w_db = set()
    for i, d in enumerate(dashboards_id):
        logging.info("Getting info for dashboard %d/%d.", i + 1, len(dashboards_id))
        res = superset.request("GET", f"/dashboard/{d}")
        result = res["result"]

        dashboard_id = result["id"]
        title = result["dashboard_title"]
        url = superset.url + "/superset/dashboard/" + str(dashboard_id)
        owner_name = result["owners"][0]["first_name"] + " " + result["owners"][0]["last_name"]

        # take unique dataset names, formatted as "[database].[schema].[table]" by Superset
        res_table_names = superset.request("GET", f"/dashboard/{d}/datasets")
        result_table_names = res_table_names["result"]

        testing = []
        for i in range(0, len(result_table_names)):
            testing.append(result_table_names[i]["name"])

        # datasets_raw = list(set(result['table_names'].split(', ')))
        datasets_raw = testing

        # parse dataset names into parts
        datasets_parsed = [dataset[1:-1].split("].[", maxsplit=2) for dataset in datasets_raw]
        datasets_parsed = [
            [dataset[0], "None", dataset[1]]  # add None in the middle
            if len(dataset) == 2
            else dataset  # if missing the schema
            for dataset in datasets_parsed
        ]

        # put them all back together to get "database.schema.table"
        datasets_w_db = [".".join(dataset) for dataset in datasets_parsed]

        dbt_project_name = "your_dbt_project."
        datasets_w_db = [dbt_project_name + sub for sub in testing]

        dashboards_datasets_w_db.update(datasets_w_db)

        # skip database, i.e. first item, to get only "schema.table"
        datasets_wo_db = [".".join(dataset[1:]) for dataset in datasets_parsed]

        datasets_wo_db = testing
        dashboard = {
            "id": dashboard_id,
            "title": title,
            "url": url,
            "owner_name": owner_name,
            "owner_email": "",  # required for dbt to accept owner_name but not in response
            "datasets": datasets_wo_db,  # add in "schema.table" format
        }
        dashboards.append(dashboard)
    # test if unique when database disregarded
    # loop to get the name of duplicated dataset and work with unique set of datasets w db
    dashboards_datasets = set()
    for dataset_w_db in dashboards_datasets_w_db:
        dataset = ".".join(dataset_w_db.split(".")[1:])  # similar logic as just a bit above

        # fail if it breaks uniqueness constraint and not limited to one database
        assert dataset not in dashboards_datasets or superset_db_id is not None, (
            f"Dataset {dataset} is a duplicate name (schema + table) across databases. "
            "This would result in incorrect matching between Superset and dbt. "
            "To fix this, remove duplicates or add ``superset_db_id``."
        )

        dashboards_datasets.add(dataset)

    return dashboards, dashboards_datasets


def get_datasets_from_superset_dbt_refs(
    superset: SupersetDBTConnectorSession, dashboards_datasets, dbt_tables, sql_dialect, superset_db_id
):
    """
    Returns datasets (dict) containing table info and dbt references
    """

    logging.info("Getting datasets info from Superset.")
    page_number = 0
    datasets = {}
    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request("GET", f'/dataset/?q={{"page":{page_number},"page_size":100}}')
        result = res["result"]
        if result:
            for r in result:
                name = r["table_name"]
                schema = r["schema"]
                database_name = r["database"]["database_name"]
                database_id = r["database"]["id"]

                dataset_key = f"{schema}.{name}"  # same format as in dashboards

                # only add datasets that are in dashboards, optionally limit to one database
                if dataset_key in dashboards_datasets and (superset_db_id is None or database_id == superset_db_id):
                    kind = r["kind"]
                    if kind == "virtual":  # built on custom sql
                        sql = r["sql"]
                        tables = get_tables_from_sql(sql, sql_dialect)
                        tables = [table if "." in table else f"{schema}.{table}" for table in tables]
                    else:  # built on tables
                        tables = [dataset_key]
                    dbt_refs = [dbt_tables[table]["ref"] for table in tables if table in dbt_tables]

                    datasets[dataset_key] = {
                        "name": name,
                        "schema": schema,
                        "database": database_name,
                        "kind": kind,
                        "tables": tables,
                        "dbt_refs": dbt_refs,
                    }
            page_number += 1
        else:
            break

    return datasets


def get_tables_descriptions_from_dbt(dbt_manifest, dbt_db_name):
    global table
    tables = {}
    for table_type in ["nodes"]:
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table["name"]
            schema = table["schema"]
            database = table["database"]
            description = table["description"]
            table_key_short = schema + "." + name
            columns = table["columns"]

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key_short not in tables, (
                    f"Table {table_key_short} is a duplicate name (schema + table) "
                    f"across databases. "
                    "This would result in incorrect matching between Superset and dbt. "
                    "To fix this, remove duplicates or add the ``dbt_db_name`` argument."
                )

                tables[table_key_short] = {"columns": columns}
                tables[table_key_short]["description"] = description

    assert tables, "Manifest is empty!"

    return tables


def refresh_columns_in_superset(superset: SupersetDBTConnectorSession, dataset_id):
    logging.info("Refreshing columns in Superset.")
    superset.request("PUT", f"/dataset/{dataset_id}/refresh")


def add_certifications_in_superset(superset: SupersetDBTConnectorSession, dataset_id, sst_dataset_key, dbt_tables):
    logging.info("Refreshing columns in Superset.")
    body = {
        "extra": '{"certification": \n  {"certified_by": "Data Analytics Team", \n   "details": "This table is the source of truth." \n    \n  }\n}',
        "description": dbt_tables[sst_dataset_key]["description"],
        "owners": [1],
    }
    superset.request("PUT", f"/dataset/{dataset_id}", json=body)


def add_superset_columns(superset: SupersetDBTConnectorSession, dataset):
    logging.info("Pulling fresh columns info from Superset.")
    res = superset.request("GET", f"/dataset/{dataset['id']}")
    columns = res["result"]["columns"]
    dataset["columns"] = columns
    return dataset


def convert_markdown_to_plain_text(md_string):
    """Converts a markdown string to plaintext.

    The following solution is used:
    https://gist.github.com/lorey/eb15a7f3338f959a78cc3661fbc255fe
    """

    # md -> html -> text since BeautifulSoup can extract text cleanly
    html = markdown(md_string)

    # remove code snippets
    html = re.sub(r"<pre>(.*?)</pre>", " ", html)
    html = re.sub(r"<code>(.*?)</code >", " ", html)

    # extract text
    soup = BeautifulSoup(html, "html.parser")
    text = "".join(soup.findAll(text=True))

    # make one line
    single_line = re.sub(r"\s+", " ", text)

    # make fixes
    single_line = re.sub("→", "->", single_line)
    single_line = re.sub("<null>", '"null"', single_line)

    return single_line


def merge_columns_info(dataset, tables):
    logging.info("Merging columns info from Superset and manifest.json file.")

    key = dataset["key"]
    sst_columns = dataset["columns"]
    dbt_columns = tables.get(key, {}).get("columns", {})
    columns_new = []
    for sst_column in sst_columns:
        # add the mandatory field
        column_new = {"column_name": sst_column["column_name"]}

        # add optional fields only if not already None, otherwise it would error out
        for field in [
            "expression",
            "filterable",
            "groupby",
            "python_date_format",
            "verbose_name",
            "type",
            "is_dttm",
            "is_active",
        ]:
            if sst_column[field] is not None:
                column_new[field] = sst_column[field]

        # add description
        if (
            sst_column["column_name"] in dbt_columns
            and "description" in dbt_columns[sst_column["column_name"]]
            and sst_column["expression"] == ""
        ):  # database columns
            description = dbt_columns[sst_column["column_name"]]["description"]
            description = convert_markdown_to_plain_text(description)
        else:  # if cant find in dbt
            description = sst_column["description"]
        column_new["description"] = description

        columns_new.append(column_new)

    dataset["columns_new"] = columns_new

    return dataset


def put_columns_to_superset(superset: SupersetDBTConnectorSession, dataset):
    logging.info("Putting new columns info with descriptions back into Superset.")
    body = {"columns": dataset["columns_new"]}
    superset.request("PUT", f"/dataset/{dataset['id']}?override_columns=true", json=body)


def merge_dashboards_with_datasets(dashboards, datasets):
    for dashboard in dashboards:
        refs = set()
        for dataset in dashboard["datasets"]:
            if dataset in datasets:
                refs.update(datasets[dataset]["dbt_refs"])
        refs = list(sorted(refs))

        dashboard["refs"] = refs

    return dashboards


def get_exposures_dict(dashboards, exposures):
    dashboards.sort(key=lambda dashboard: dashboard["id"])
    titles = [dashboard["title"] for dashboard in dashboards]
    # fail if it breaks uniqueness constraint for exposure names
    assert len(set(titles)) == len(titles), "There are duplicate dashboard names!"

    exposures_orig = {exposure["url"]: exposure for exposure in exposures}
    exposures_dict = [
        {
            "name": f"superset__{dashboard['title']}",
            "type": "dashboard",
            "url": dashboard["url"],
            "description": exposures_orig.get(dashboard["url"], {}).get("description", ""),
            "depends_on": dashboard["refs"],
            "owner": {"name": dashboard["owner_name"], "email": dashboard["owner_email"]},
        }
        for dashboard in dashboards
    ]

    return exposures_dict


class YamlFormatted(ruamel.yaml.YAML):
    def __init__(self):
        super(YamlFormatted, self).__init__()
        self.default_flow_style = False
        self.allow_unicode = True
        self.encoding = "utf-8"
        self.block_seq_indent = 2
        self.indent = 4
        self.emitter.alt_null = ""