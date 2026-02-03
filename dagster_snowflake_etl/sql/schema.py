"""SQL templates for schema creation."""


def get_raw_schema_ddl(database: str, schema: str) -> list[str]:
    """Generate DDL for RAW layer schema and tables."""
    return [
        f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}",
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.RAW_DATA (
            DOC VARIANT NOT NULL,
            KEY VARCHAR NOT NULL,
            BATCH_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            SOURCE VARCHAR NOT NULL
        )
        """,
    ]


def get_psa_schema_ddl(database: str, schema: str) -> list[str]:
    """Generate DDL for PSA (Persistent Staging Area) layer schema and tables."""
    return [
        f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}",
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.PSA_DATA (
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
        """,
    ]


def get_prep_schema_ddl(database: str, prep_schema: str, psa_schema: str) -> list[str]:
    """Generate DDL for PREP layer schema and views.

    PREP layer shows only active records (END_TS year = 9999) in tabular format.
    """
    return [
        f"CREATE SCHEMA IF NOT EXISTS {database}.{prep_schema}",
        f"""
        CREATE OR REPLACE VIEW {database}.{prep_schema}.PREP_DATA AS
        SELECT
            DOC,
            DOC_HASH,
            KEY_DOC,
            KEY_DOC_HASH,
            START_TS,
            END_TS,
            BATCH_TS,
            SOURCE
        FROM {database}.{psa_schema}.PSA_DATA
        WHERE YEAR(END_TS) = 9999
        """,
    ]


def get_pres_schema_ddl(database: str, pres_schema: str, prep_schema: str) -> list[str]:
    """Generate DDL for PRES (Presentation) layer schema and views.

    PRES layer is an abstraction over PREP layer for consumers.
    """
    return [
        f"CREATE SCHEMA IF NOT EXISTS {database}.{pres_schema}",
        f"""
        CREATE OR REPLACE VIEW {database}.{pres_schema}.PRES_DATA AS
        SELECT *
        FROM {database}.{prep_schema}.PREP_DATA
        """,
    ]
