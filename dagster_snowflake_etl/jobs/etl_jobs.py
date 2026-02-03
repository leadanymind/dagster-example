"""Dagster jobs for ETL orchestration."""

from dagster import define_asset_job, AssetSelection


# Job to set up all schema layers
schema_setup_job = define_asset_job(
    name="schema_setup_job",
    selection=AssetSelection.groups("schema_setup"),
    description="Creates all schema layers (RAW, PSA, PREP, PRES) in Snowflake",
)

# Job to set up governance (roles and permissions)
governance_setup_job = define_asset_job(
    name="governance_setup_job",
    selection=AssetSelection.groups("governance"),
    description="Creates data governance roles and grants schema permissions",
)

# Job to do full initial setup (schema + governance)
full_setup_job = define_asset_job(
    name="full_setup_job",
    selection=AssetSelection.groups("schema_setup", "governance"),
    description="Full initial setup: creates schemas and sets up governance",
)

# Job to run ETL pipeline (RAW to PSA merge)
etl_pipeline_job = define_asset_job(
    name="etl_pipeline_job",
    selection=AssetSelection.groups("etl"),
    description="Runs ETL pipeline: merges data from RAW to PSA layer",
)
