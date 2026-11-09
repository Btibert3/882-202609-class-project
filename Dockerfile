FROM astrocrpublic.azurecr.io/runtime:3.3-8
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/plugins"

# Install dbt-bigquery into a virtualenv to avoid dependency conflicts with Airflow
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate
