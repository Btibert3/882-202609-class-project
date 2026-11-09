"""
## AutoElite: dbt Transformation DAG (via Cosmos)

Runs the full dbt project as an Airflow DAG using astronomer-cosmos.
Each dbt model becomes its own Airflow task — dependencies between models
are wired automatically from ref() calls in the dbt project.

Replaces the manual `dbt run` pattern. Runs daily after ingestion pipelines
have landed raw data.
"""

from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ExecutionMode, ProjectConfig, ProfileConfig

DBT_PROJECT_PATH = (Path(__file__).parent / "../include/dbt").absolute()

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
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    schedule="0 11 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["dbt", "transform"],
)
