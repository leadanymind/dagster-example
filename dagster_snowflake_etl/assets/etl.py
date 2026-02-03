"""Assets for ETL operations - RAW to PSA merge."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue, Config

from dagster_snowflake_etl.resources.snowflake import SnowflakeResource
from dagster_snowflake_etl.sql.etl import (
    get_raw_to_psa_merge_sql,
    get_close_changed_records_sql,
    get_truncate_raw_sql,
)


class RawToPsaMergeConfig(Config):
    """Configuration for RAW to PSA merge operation."""

    raw_table: str = "RAW_DATA"
    psa_table: str = "PSA_DATA"
    truncate_raw_after_merge: bool = False


@asset(
    group_name="etl",
    description="Merges data from RAW layer into PSA layer with SCD Type 2 logic",
)
def raw_to_psa_merge(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    config: RawToPsaMergeConfig,
    psa_schema: dict,
) -> Output[dict]:
    """Merge data from RAW layer into PSA layer.

    Implements SCD Type 2 (Slowly Changing Dimension):
    1. Close existing records when document hash changes (set END_TS)
    2. Insert new records for new keys or changed documents

    The PREP and PRES views automatically reflect changes since they are
    views on top of PSA filtering for active records (END_TS year = 9999).
    """
    database = snowflake.database
    raw_schema = snowflake.raw_schema
    psa_schema_name = snowflake.psa_schema

    context.log.info(
        f"Starting RAW to PSA merge: {database}.{raw_schema}.{config.raw_table} -> "
        f"{database}.{psa_schema_name}.{config.psa_table}"
    )

    # Step 1: Close changed records (SCD Type 2)
    close_sql = get_close_changed_records_sql(
        database=database,
        raw_schema=raw_schema,
        psa_schema=psa_schema_name,
        raw_table=config.raw_table,
        psa_table=config.psa_table,
    )
    context.log.info("Closing changed records in PSA...")
    result = snowflake.execute_sql(close_sql)
    records_closed = result[0][0] if result else 0
    context.log.info(f"Closed {records_closed} changed records")

    # Step 2: Merge new/changed records
    merge_sql = get_raw_to_psa_merge_sql(
        database=database,
        raw_schema=raw_schema,
        psa_schema=psa_schema_name,
        raw_table=config.raw_table,
        psa_table=config.psa_table,
    )
    context.log.info("Merging new records from RAW to PSA...")
    result = snowflake.execute_sql(merge_sql)
    records_merged = result[0][0] if result else 0
    context.log.info(f"Merged {records_merged} records")

    # Step 3: Optionally truncate RAW table
    records_truncated = False
    if config.truncate_raw_after_merge:
        truncate_sql = get_truncate_raw_sql(
            database=database,
            raw_schema=raw_schema,
            raw_table=config.raw_table,
        )
        context.log.info("Truncating RAW table...")
        snowflake.execute_sql(truncate_sql)
        records_truncated = True
        context.log.info("RAW table truncated")

    context.log.info("RAW to PSA merge completed successfully")

    return Output(
        value={
            "records_closed": records_closed,
            "records_merged": records_merged,
            "raw_truncated": records_truncated,
        },
        metadata={
            "source_table": MetadataValue.text(
                f"{database}.{raw_schema}.{config.raw_table}"
            ),
            "target_table": MetadataValue.text(
                f"{database}.{psa_schema_name}.{config.psa_table}"
            ),
            "records_closed": MetadataValue.int(records_closed),
            "records_merged": MetadataValue.int(records_merged),
            "raw_truncated": MetadataValue.bool(records_truncated),
        },
    )
