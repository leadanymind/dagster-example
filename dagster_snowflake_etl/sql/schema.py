"""SQL templates for schema creation."""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagster_snowflake_etl.config.datasets import EntityConfig


def get_schema_ddl(database: str, schema: str) -> str:
    """Generate DDL for creating a schema."""
    return f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}"


def get_raw_table_ddl(database: str, schema: str, entity: EntityConfig) -> str:
    """Generate DDL for a RAW layer table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{entity.raw_table} (
        DOC VARIANT NOT NULL,
        KEY VARCHAR NOT NULL,
        BATCH_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
        SOURCE VARCHAR NOT NULL
    )
    """


def get_psa_table_ddl(database: str, schema: str, entity: EntityConfig) -> str:
    """Generate DDL for a PSA layer table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{entity.psa_table} (
        DOC VARIANT NOT NULL,
        DOC_HASH BINARY NOT NULL,
        KEY_DOC VARIANT NOT NULL,
        KEY_DOC_HASH BINARY NOT NULL,
        START_TS TIMESTAMP_LTZ NOT NULL,
        END_TS TIMESTAMP_LTZ NOT NULL DEFAULT '9999-12-31'::TIMESTAMP_LTZ,
        BATCH_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
        SOURCE VARCHAR NOT NULL,
        PRIMARY KEY (KEY_DOC_HASH, START_TS)
    )
    """


def get_prep_view_ddl(
    database: str,
    prep_schema: str,
    psa_schema: str,
    entity: EntityConfig,
) -> str:
    """Generate DDL for a PREP layer view.

    If prep_columns are defined, extracts those columns from DOC.
    Otherwise, returns the raw PSA columns.
    """
    if entity.prep_columns:
        column_expressions = ",\n            ".join(
            f"{col['expression']} AS {col['name']}"
            for col in entity.prep_columns
        )
        select_clause = f"""
            {column_expressions},
            START_TS,
            END_TS,
            BATCH_TS,
            SOURCE"""
    else:
        select_clause = """
            DOC,
            DOC_HASH,
            KEY_DOC,
            KEY_DOC_HASH,
            START_TS,
            END_TS,
            BATCH_TS,
            SOURCE"""

    return f"""
    CREATE OR REPLACE VIEW {database}.{prep_schema}.{entity.prep_view} AS
    SELECT{select_clause}
    FROM {database}.{psa_schema}.{entity.psa_table}
    WHERE YEAR(END_TS) = 9999
    """


def get_pres_view_ddl(
    database: str,
    pres_schema: str,
    prep_schema: str,
    entity: EntityConfig,
) -> str:
    """Generate DDL for a PRES layer view."""
    return f"""
    CREATE OR REPLACE VIEW {database}.{pres_schema}.{entity.pres_view} AS
    SELECT *
    FROM {database}.{prep_schema}.{entity.prep_view}
    """


# Legacy functions for backward compatibility (schema-level setup)
def get_raw_schema_ddl(database: str, schema: str) -> list[str]:
    """Generate DDL for RAW layer schema only."""
    return [get_schema_ddl(database, schema)]


def get_psa_schema_ddl(database: str, schema: str) -> list[str]:
    """Generate DDL for PSA layer schema only."""
    return [get_schema_ddl(database, schema)]


def get_prep_schema_ddl(database: str, prep_schema: str, psa_schema: str) -> list[str]:
    """Generate DDL for PREP layer schema only."""
    return [get_schema_ddl(database, prep_schema)]


def get_pres_schema_ddl(database: str, pres_schema: str, prep_schema: str) -> list[str]:
    """Generate DDL for PRES layer schema only."""
    return [get_schema_ddl(database, pres_schema)]
