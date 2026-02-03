"""Assets for setting up Snowflake schema layers."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue

from dagster_snowflake_etl.resources.snowflake import SnowflakeResource
from dagster_snowflake_etl.sql.schema import (
    get_raw_schema_ddl,
    get_psa_schema_ddl,
    get_prep_schema_ddl,
    get_pres_schema_ddl,
)


@asset(
    group_name="schema_setup",
    description="Creates the RAW layer schema and tables for staging data",
)
def raw_schema(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Create RAW layer schema and tables.

    RAW layer schema: DOC=variant, KEY=varchar, BATCH_TS=timestamp_ltz, SOURCE=varchar
    """
    database = snowflake.database
    schema = snowflake.raw_schema

    ddl_statements = get_raw_schema_ddl(database, schema)
    context.log.info(f"Creating RAW schema: {database}.{schema}")

    for stmt in ddl_statements:
        context.log.debug(f"Executing: {stmt[:100]}...")

    snowflake.execute_sql_multi(ddl_statements)

    context.log.info(f"RAW schema created successfully: {database}.{schema}")

    return Output(
        value={"database": database, "schema": schema, "layer": "RAW"},
        metadata={
            "database": MetadataValue.text(database),
            "schema": MetadataValue.text(schema),
            "statements_executed": MetadataValue.int(len(ddl_statements)),
        },
    )


@asset(
    group_name="schema_setup",
    deps=[raw_schema],
    description="Creates the PSA (Persistent Staging Area) layer schema and tables",
)
def psa_schema(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Create PSA layer schema and tables.

    PSA layer schema: DOC=variant, DOC_HASH=binary, KEY_DOC=variant,
    KEY_DOC_HASH=binary, START_TS=timestamp_ltz, END_TS=timestamp_ltz,
    BATCH_TS=timestamp_ltz, SOURCE=varchar
    Primary key: KEY_DOC_HASH, START_TS
    """
    database = snowflake.database
    schema = snowflake.psa_schema

    ddl_statements = get_psa_schema_ddl(database, schema)
    context.log.info(f"Creating PSA schema: {database}.{schema}")

    for stmt in ddl_statements:
        context.log.debug(f"Executing: {stmt[:100]}...")

    snowflake.execute_sql_multi(ddl_statements)

    context.log.info(f"PSA schema created successfully: {database}.{schema}")

    return Output(
        value={"database": database, "schema": schema, "layer": "PSA"},
        metadata={
            "database": MetadataValue.text(database),
            "schema": MetadataValue.text(schema),
            "statements_executed": MetadataValue.int(len(ddl_statements)),
        },
    )


@asset(
    group_name="schema_setup",
    deps=[psa_schema],
    description="Creates the PREP layer schema and views for active data",
)
def prep_schema(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Create PREP layer schema and views.

    PREP layer provides a tabular view of only the active data
    (records where END_TS year is 9999).
    """
    database = snowflake.database
    prep = snowflake.prep_schema
    psa = snowflake.psa_schema

    ddl_statements = get_prep_schema_ddl(database, prep, psa)
    context.log.info(f"Creating PREP schema: {database}.{prep}")

    for stmt in ddl_statements:
        context.log.debug(f"Executing: {stmt[:100]}...")

    snowflake.execute_sql_multi(ddl_statements)

    context.log.info(f"PREP schema created successfully: {database}.{prep}")

    return Output(
        value={"database": database, "schema": prep, "layer": "PREP"},
        metadata={
            "database": MetadataValue.text(database),
            "schema": MetadataValue.text(prep),
            "statements_executed": MetadataValue.int(len(ddl_statements)),
        },
    )


@asset(
    group_name="schema_setup",
    deps=[prep_schema],
    description="Creates the PRES (Presentation) layer schema and views",
)
def pres_schema(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Create PRES layer schema and views.

    PRES layer is an abstraction that selects * from PREP layer views.
    """
    database = snowflake.database
    pres = snowflake.pres_schema
    prep = snowflake.prep_schema

    ddl_statements = get_pres_schema_ddl(database, pres, prep)
    context.log.info(f"Creating PRES schema: {database}.{pres}")

    for stmt in ddl_statements:
        context.log.debug(f"Executing: {stmt[:100]}...")

    snowflake.execute_sql_multi(ddl_statements)

    context.log.info(f"PRES schema created successfully: {database}.{pres}")

    return Output(
        value={"database": database, "schema": pres, "layer": "PRES"},
        metadata={
            "database": MetadataValue.text(database),
            "schema": MetadataValue.text(pres),
            "statements_executed": MetadataValue.int(len(ddl_statements)),
        },
    )
