"""
## AutoElite: dbt Transformation DAG (via Cosmos)

Runs the full dbt project as an Airflow DAG using astronomer-cosmos.
Each dbt model becomes its own Airflow task — dependencies between models
are wired automatically from ref() calls in the dbt project.

Replaces the manual `dbt run` pattern. Runs daily after ingestion pipelines
have landed raw data.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ExecutionMode, ProjectConfig, ProfileConfig

DBT_PROJECT_PATH = (Path(__file__).parent / "../include/dbt").absolute()

# Local standalone (Cloud Shell): ~/.local/bin/dbt
# Astronomer managed (Docker): /usr/local/airflow/dbt_venv/bin/dbt
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")

dbt_autoelite = DbtDag(
    dag_id="dbt_autoelite",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name="autoelite",
        target_name="dev",
        profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path=DBT_EXECUTABLE,
    ),
    schedule="0 11 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["dbt", "transform"],
)
