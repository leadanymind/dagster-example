"""Dagster definitions entry point."""

import os

from dagster import Definitions, EnvVar
from dotenv import load_dotenv

from dagster_snowflake_etl.resources.snowflake import SnowflakeResource
from dagster_snowflake_etl.assets import (
    raw_schema,
    psa_schema,
    prep_schema,
    pres_schema,
    data_governance_roles,
    schema_permissions,
    raw_to_psa_merge,
)
from dagster_snowflake_etl.jobs import (
    schema_setup_job,
    governance_setup_job,
    full_setup_job,
    etl_pipeline_job,
)

# Load environment variables from .env file
load_dotenv()

defs = Definitions(
    assets=[
        # Schema setup assets
        raw_schema,
        psa_schema,
        prep_schema,
        pres_schema,
        # Governance assets
        data_governance_roles,
        schema_permissions,
        # ETL assets
        raw_to_psa_merge,
    ],
    jobs=[
        schema_setup_job,
        governance_setup_job,
        full_setup_job,
        etl_pipeline_job,
    ],
    resources={
        "snowflake": SnowflakeResource(
            account=EnvVar("SNOWFLAKE_ACCOUNT"),
            user=EnvVar("SNOWFLAKE_USER"),
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
            database=EnvVar("SNOWFLAKE_DATABASE"),
            role=EnvVar("SNOWFLAKE_ROLE"),
            raw_schema=os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW"),
            psa_schema=os.getenv("SNOWFLAKE_PSA_SCHEMA", "PSA"),
            prep_schema=os.getenv("SNOWFLAKE_PREP_SCHEMA", "PREP"),
            pres_schema=os.getenv("SNOWFLAKE_PRES_SCHEMA", "PRES"),
        ),
    },
)
