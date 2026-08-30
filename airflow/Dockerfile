# Airflow's own Python environment -- deliberately separate from the
# project's uv venv. Airflow pins strict dependency constraints, so it needs
# dbt installed into its own image rather than sharing .venv.
#
# Airflow 3.3.1 is current latest as of 2026-08-26 (apache/airflow:3.3.1-
# python3.12 on Docker Hub, published 2026-08-12). Bump this by checking
# https://hub.docker.com/r/apache/airflow/tags -- Airflow does not publish a
# rolling `latest` tag, every version has to be pinned explicitly.
FROM apache/airflow:3.3.1-python3.12

# Same pins as pyproject.toml, for the same reason -- see "1. Pin dbt-core
# below 1.12, or resolution fails" in the README. dbt-databricks 1.12.4 (the
# current latest) still requires dbt-core>=1.11.2,<1.12.1 -- confirmed against
# its PyPI Requires-Dist directly, not assumed. dbt-core's own latest (1.12.3)
# is NOT installable here: it is past dbt-databricks's <1.12.1 ceiling.
#
# If this RUN ever fails on a dependency conflict with Airflow's own pinned
# packages (Airflow's dependency footprint is large and strict), the fix is
# an isolated virtualenv for dbt inside the container -- e.g. astronomer-
# cosmos's ExecutionMode.VIRTUALENV -- rather than loosening either pin above.
RUN pip install --no-cache-dir \
    "dbt-core<1.12" \
    "dbt-databricks>=1.12.4" \
    "databricks-sdk>=0.117.0"
