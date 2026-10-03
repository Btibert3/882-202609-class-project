FROM astrocrpublic.azurecr.io/runtime:3.3-8
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/plugins"
