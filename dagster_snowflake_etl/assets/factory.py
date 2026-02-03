"""Asset factory for dynamically generating assets from configuration."""

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    AssetsDefinition,
    AssetKey,
)

from dagster_snowflake_etl.config.datasets import DatasetConfig, DatabaseConfig, EntityConfig
from dagster_snowflake_etl.resources.snowflake import SnowflakeResource
from dagster_snowflake_etl.sql.schema import (
    get_schema_ddl,
    get_raw_table_ddl,
    get_psa_table_ddl,
    get_prep_view_ddl,
    get_pres_view_ddl,
)
from dagster_snowflake_etl.sql.etl import (
    get_entity_merge_sql,
    get_entity_close_sql,
    get_entity_truncate_sql,
)
from dagster_snowflake_etl.sql.governance import (
    get_role_ddl,
    get_database_permissions,
)


def _make_asset_key(db_name: str, *parts: str) -> str:
    """Create a consistent asset key name."""
    return f"{db_name.lower()}_{'_'.join(parts)}"


def create_database_schema_asset(db_config: DatabaseConfig) -> AssetsDefinition:
    """Create an asset that sets up all schemas for a database."""

    @asset(
        name=_make_asset_key(db_config.name, "schemas"),
        group_name=db_config.name.lower(),
        description=f"Creates RAW/PSA/PREP/PRES schemas for {db_config.name}",
    )
    def database_schemas(
        context: AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> Output[dict]:
        ddl_statements = [
            get_schema_ddl(db_config.name, db_config.raw_schema),
            get_schema_ddl(db_config.name, db_config.psa_schema),
            get_schema_ddl(db_config.name, db_config.prep_schema),
            get_schema_ddl(db_config.name, db_config.pres_schema),
        ]

        context.log.info(f"Creating schemas for database: {db_config.name}")
        snowflake.execute_sql_multi(ddl_statements)

        return Output(
            value={"database": db_config.name, "schemas_created": 4},
            metadata={
                "database": MetadataValue.text(db_config.name),
                "schemas": MetadataValue.json([
                    db_config.raw_schema,
                    db_config.psa_schema,
                    db_config.prep_schema,
                    db_config.pres_schema,
                ]),
            },
        )

    return database_schemas


def create_entity_tables_asset(
    db_config: DatabaseConfig,
    entity: EntityConfig,
) -> AssetsDefinition:
    """Create an asset that sets up tables/views for an entity."""
    schema_asset_key = _make_asset_key(db_config.name, "schemas")

    @asset(
        name=_make_asset_key(db_config.name, entity.name, "tables"),
        group_name=db_config.name.lower(),
        deps=[AssetKey(schema_asset_key)],
        description=f"Creates tables and views for {db_config.name}.{entity.name}",
    )
    def entity_tables(
        context: AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> Output[dict]:
        ddl_statements = [
            get_raw_table_ddl(db_config.name, db_config.raw_schema, entity),
            get_psa_table_ddl(db_config.name, db_config.psa_schema, entity),
            get_prep_view_ddl(
                db_config.name, db_config.prep_schema, db_config.psa_schema, entity
            ),
            get_pres_view_ddl(
                db_config.name, db_config.pres_schema, db_config.prep_schema, entity
            ),
        ]

        context.log.info(
            f"Creating tables/views for {db_config.name}.{entity.name}"
        )
        for stmt in ddl_statements:
            context.log.debug(f"Executing: {stmt[:80]}...")

        snowflake.execute_sql_multi(ddl_statements)

        return Output(
            value={
                "database": db_config.name,
                "entity": entity.name,
                "objects_created": 4,
            },
            metadata={
                "database": MetadataValue.text(db_config.name),
                "entity": MetadataValue.text(entity.name),
                "raw_table": MetadataValue.text(
                    f"{db_config.name}.{db_config.raw_schema}.{entity.raw_table}"
                ),
                "psa_table": MetadataValue.text(
                    f"{db_config.name}.{db_config.psa_schema}.{entity.psa_table}"
                ),
                "prep_view": MetadataValue.text(
                    f"{db_config.name}.{db_config.prep_schema}.{entity.prep_view}"
                ),
                "pres_view": MetadataValue.text(
                    f"{db_config.name}.{db_config.pres_schema}.{entity.pres_view}"
                ),
            },
        )

    return entity_tables


def create_entity_merge_asset(
    db_config: DatabaseConfig,
    entity: EntityConfig,
) -> AssetsDefinition:
    """Create an asset that merges RAW to PSA for an entity."""
    tables_asset_key = _make_asset_key(db_config.name, entity.name, "tables")

    @asset(
        name=_make_asset_key(db_config.name, entity.name, "merge"),
        group_name=db_config.name.lower(),
        deps=[AssetKey(tables_asset_key)],
        description=f"Merges {entity.name} from RAW to PSA with SCD Type 2",
    )
    def entity_merge(
        context: AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> Output[dict]:
        context.log.info(
            f"Merging {db_config.name}.{entity.name} from RAW to PSA"
        )

        # Close changed records
        close_sql = get_entity_close_sql(db_config, entity)
        context.log.debug("Closing changed records...")
        snowflake.execute_sql(close_sql)

        # Merge new/changed records
        merge_sql = get_entity_merge_sql(db_config, entity)
        context.log.debug("Merging records...")
        result = snowflake.execute_sql(merge_sql)
        records_merged = result[0][0] if result else 0

        context.log.info(f"Merged {records_merged} records for {entity.name}")

        return Output(
            value={
                "database": db_config.name,
                "entity": entity.name,
                "records_merged": records_merged,
            },
            metadata={
                "database": MetadataValue.text(db_config.name),
                "entity": MetadataValue.text(entity.name),
                "records_merged": MetadataValue.int(records_merged),
            },
        )

    return entity_merge


def create_database_governance_asset(db_config: DatabaseConfig) -> AssetsDefinition:
    """Create an asset that sets up permissions for a database."""
    schema_asset_key = _make_asset_key(db_config.name, "schemas")

    @asset(
        name=_make_asset_key(db_config.name, "governance"),
        group_name=db_config.name.lower(),
        deps=[AssetKey(schema_asset_key)],
        description=f"Grants permissions for {db_config.name}",
    )
    def database_governance(
        context: AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> Output[dict]:
        grant_statements = get_database_permissions(
            database=db_config.name,
            raw_schema=db_config.raw_schema,
            psa_schema=db_config.psa_schema,
            prep_schema=db_config.prep_schema,
            pres_schema=db_config.pres_schema,
        )

        context.log.info(f"Granting permissions for database: {db_config.name}")
        snowflake.execute_sql_multi(grant_statements)

        return Output(
            value={
                "database": db_config.name,
                "grants_executed": len(grant_statements),
            },
            metadata={
                "database": MetadataValue.text(db_config.name),
                "grants_executed": MetadataValue.int(len(grant_statements)),
            },
        )

    return database_governance


def create_global_roles_asset() -> AssetsDefinition:
    """Create an asset that sets up global governance roles."""

    @asset(
        name="global_governance_roles",
        group_name="governance",
        description="Creates global data governance roles",
    )
    def global_roles(
        context: AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> Output[dict]:
        ddl_statements = get_role_ddl()

        context.log.info("Creating global governance roles...")
        snowflake.execute_sql_multi(ddl_statements)

        roles_created = [
            "ETL_SERVICE_ROLE",
            "DATA_ANALYST_ROLE",
            "DATA_ENGINEER_ROLE",
            "DATA_READER_ROLE",
        ]

        return Output(
            value={"roles_created": roles_created},
            metadata={
                "roles": MetadataValue.json(roles_created),
            },
        )

    return global_roles


def build_assets_from_config(config: DatasetConfig) -> list[AssetsDefinition]:
    """Build all assets from the dataset configuration.

    This is the main entry point for generating assets dynamically.

    Returns:
        List of all generated assets:
        - Global governance roles
        - Per-database: schemas, governance
        - Per-entity: tables, merge
    """
    assets: list[AssetsDefinition] = []

    # Global roles asset
    assets.append(create_global_roles_asset())

    # Per-database assets
    for db_config in config.databases:
        # Database schema setup
        assets.append(create_database_schema_asset(db_config))

        # Database governance (permissions)
        assets.append(create_database_governance_asset(db_config))

        # Per-entity assets
        for entity in db_config.entities:
            # Entity tables and views
            assets.append(create_entity_tables_asset(db_config, entity))

            # Entity merge (RAW to PSA)
            assets.append(create_entity_merge_asset(db_config, entity))

    return assets
