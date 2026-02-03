"""Dagster definitions entry point."""

import os
from pathlib import Path

from dagster import Definitions, EnvVar, define_asset_job, AssetSelection
from dotenv import load_dotenv

from dagster_snowflake_etl.resources.snowflake import SnowflakeResource
from dagster_snowflake_etl.config.datasets import load_datasets_config
from dagster_snowflake_etl.assets.factory import build_assets_from_config

# Load environment variables from .env file
load_dotenv()

# Load dataset configuration
CONFIG_PATH = os.getenv(
    "DATASETS_CONFIG_PATH",
    str(Path(__file__).parent.parent / "config" / "datasets.yaml"),
)
datasets_config = load_datasets_config(CONFIG_PATH)

# Build assets dynamically from configuration
all_assets = build_assets_from_config(datasets_config)

# Create jobs dynamically for each database
jobs = []

# Global governance job
jobs.append(
    define_asset_job(
        name="global_governance_job",
        selection=AssetSelection.groups("governance"),
        description="Creates global data governance roles",
    )
)

# Per-database jobs
for db_config in datasets_config.databases:
    db_name = db_config.name.lower()

    # Full database setup job (schemas + governance + all entity tables)
    jobs.append(
        define_asset_job(
            name=f"{db_name}_setup_job",
            selection=AssetSelection.groups(db_name),
            description=f"Full setup for {db_config.name}: schemas, tables, governance",
        )
    )

    # Per-entity merge jobs
    for entity in db_config.entities:
        entity_name = entity.name.lower()
        jobs.append(
            define_asset_job(
                name=f"{db_name}_{entity_name}_merge_job",
                selection=AssetSelection.keys(f"{db_name}_{entity_name}_merge"),
                description=f"Merge {entity.name} from RAW to PSA in {db_config.name}",
            )
        )

# All merges job (runs all entity merges across all databases)
all_merge_keys = [
    f"{db.name.lower()}_{entity.name.lower()}_merge"
    for db in datasets_config.databases
    for entity in db.entities
]
if all_merge_keys:
    jobs.append(
        define_asset_job(
            name="all_merges_job",
            selection=AssetSelection.keys(*all_merge_keys),
            description="Run all RAW to PSA merges across all databases",
        )
    )

# Full setup job (everything)
jobs.append(
    define_asset_job(
        name="full_setup_job",
        selection=AssetSelection.all(),
        description="Complete setup: all databases, schemas, tables, and governance",
    )
)

defs = Definitions(
    assets=all_assets,
    jobs=jobs,
    resources={
        "snowflake": SnowflakeResource(
            account=EnvVar("SNOWFLAKE_ACCOUNT"),
            user=EnvVar("SNOWFLAKE_USER"),
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
            role=EnvVar("SNOWFLAKE_ROLE"),
        ),
    },
)
