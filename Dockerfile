# ARG BASE_IMAGE
FROM ubuntu:jammy

# ARG DAGSTER_VERSION

RUN apt-get update && apt-get install -y \
    python3-pip

RUN \
    pip3 install \
        dagster==1.3.9 \
        # dagit==1.3.9 \
        dagster-airbyte==0.19.9 \
        dagster-postgres==0.19.9 \
        dagster-dbt==0.19.9 \
        dbt-postgres==1.5.1 \
        sqlalchemy==1.4.17 \
        pydantic==1.10.10 \
	fal==0.9.2 \
	dbt-postgres==1.5.1 \
	pandas==1.5.3 \
	pathos==0.3.0 \
	tqdm==4.65.0 \
	prophet==1.1.4 \
	markdown==3.3.4 \
	ruamel.yaml==0.17.32 \
	sqlfluff==2.1.1 \
	bs4=0.0.1 \
	prophet==1.1.4 \

	
        # dagster-celery[flower,redis,kubernetes]==${DAGSTER_VERSION} \
        # dagster-aws==${DAGSTER_VERSION} \
        # dagster-k8s==${DAGSTER_VERSION} \
        # dagster-celery-k8s==${DAGSTER_VERSION} \
# Cleanup
    # &&  rm -rf /var \
    # &&  rm -rf /root/.cache  \
    # &&  rm -rf /usr/lib/python2.7 \
    # &&  rm -rf /usr/lib/x86_64-linux-gnu/guile

# ==> Add user code layer
# Example pipelines


COPY . /financial
ENV DBT_PROFILES_DIR /secret-dbt
ENV FINANCIAL_ENVIROMENT prod
