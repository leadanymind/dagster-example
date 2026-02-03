"""Assets for data governance - roles and permissions."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue

from dagster_snowflake_etl.resources.snowflake import SnowflakeResource
from dagster_snowflake_etl.sql.governance import (
    get_role_ddl,
    get_schema_permissions,
)


@asset(
    group_name="governance",
    description="Creates data governance roles for the data warehouse",
)
def data_governance_roles(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Create data governance roles.

    Roles created:
    - ETL_SERVICE_ROLE: For ETL pipelines to read/write RAW and PSA layers
    - DATA_ANALYST_ROLE: For analysts to read PREP and PRES layers
    - DATA_ENGINEER_ROLE: Full access to all layers
    - DATA_READER_ROLE: Read-only access to PRES layer
    """
    ddl_statements = get_role_ddl()

    context.log.info("Creating data governance roles...")

    for stmt in ddl_statements:
        context.log.info(f"Executing: {stmt}")

    snowflake.execute_sql_multi(ddl_statements)

    roles_created = [
        "ETL_SERVICE_ROLE",
        "DATA_ANALYST_ROLE",
        "DATA_ENGINEER_ROLE",
        "DATA_READER_ROLE",
    ]

    context.log.info(f"Successfully created {len(roles_created)} roles")

    return Output(
        value={"roles_created": roles_created},
        metadata={
            "roles": MetadataValue.json(roles_created),
            "statements_executed": MetadataValue.int(len(ddl_statements)),
        },
    )


@asset(
    group_name="governance",
    deps=[data_governance_roles],
    description="Grants schema permissions to data governance roles",
)
def schema_permissions(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    pres_schema: dict,
) -> Output[dict]:
    """Grant schema permissions to roles.

    Permission model:
    - ETL_SERVICE_ROLE: CRUD on RAW and PSA
    - DATA_ANALYST_ROLE: SELECT on PREP and PRES
    - DATA_READER_ROLE: SELECT on PRES only
    - DATA_ENGINEER_ROLE: ALL on all schemas

    Role hierarchy:
    - DATA_READER_ROLE <- DATA_ANALYST_ROLE <- DATA_ENGINEER_ROLE
    - ETL_SERVICE_ROLE <- DATA_ENGINEER_ROLE
    """
    database = snowflake.database

    grant_statements = get_schema_permissions(
        database=database,
        raw_schema=snowflake.raw_schema,
        psa_schema=snowflake.psa_schema,
        prep_schema=snowflake.prep_schema,
        pres_schema=snowflake.pres_schema,
    )

    context.log.info(f"Granting schema permissions for database: {database}")

    for stmt in grant_statements:
        context.log.debug(f"Executing: {stmt}")

    snowflake.execute_sql_multi(grant_statements)

    context.log.info(f"Successfully executed {len(grant_statements)} grant statements")

    return Output(
        value={
            "database": database,
            "grants_executed": len(grant_statements),
        },
        metadata={
            "database": MetadataValue.text(database),
            "grants_executed": MetadataValue.int(len(grant_statements)),
        },
    )
