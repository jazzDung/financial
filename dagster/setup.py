from setuptools import find_packages, setup

setup(
    name="financial",
    packages=find_packages(exclude=["financial_tests"]),
    install_requires=[
        "dagster",
        "dagit"
        "dagster-cloud",
        "dagster-airbyte",
        "dagster-dbt",
        "dbt-postgres",
        "sqlalchemy<2.0"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
