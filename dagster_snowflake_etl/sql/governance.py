"""SQL templates for data governance roles and permissions."""


def get_role_ddl() -> list[str]:
    """Generate DDL for creating data governance roles."""
    return [
        # ETL Service Role - can read/write to RAW and PSA
        "CREATE ROLE IF NOT EXISTS ETL_SERVICE_ROLE",
        # Analyst Role - can read from PREP and PRES layers
        "CREATE ROLE IF NOT EXISTS DATA_ANALYST_ROLE",
        # Data Engineer Role - can manage all layers
        "CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE",
        # Reader Role - read-only access to PRES layer
        "CREATE ROLE IF NOT EXISTS DATA_READER_ROLE",
        # Role hierarchy - Data Engineer inherits from Analyst which inherits from Reader
        "GRANT ROLE DATA_READER_ROLE TO ROLE DATA_ANALYST_ROLE",
        "GRANT ROLE DATA_ANALYST_ROLE TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ROLE ETL_SERVICE_ROLE TO ROLE DATA_ENGINEER_ROLE",
    ]


def get_database_permissions(
    database: str,
    raw_schema: str,
    psa_schema: str,
    prep_schema: str,
    pres_schema: str,
) -> list[str]:
    """Generate GRANT statements for a single database's permissions."""
    return [
        # Database usage grants
        f"GRANT USAGE ON DATABASE {database} TO ROLE ETL_SERVICE_ROLE",
        f"GRANT USAGE ON DATABASE {database} TO ROLE DATA_ANALYST_ROLE",
        f"GRANT USAGE ON DATABASE {database} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT USAGE ON DATABASE {database} TO ROLE DATA_READER_ROLE",
        # ETL Service Role permissions - RAW layer
        f"GRANT USAGE ON SCHEMA {database}.{raw_schema} TO ROLE ETL_SERVICE_ROLE",
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {database}.{raw_schema} TO ROLE ETL_SERVICE_ROLE",
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA {database}.{raw_schema} TO ROLE ETL_SERVICE_ROLE",
        # ETL Service Role permissions - PSA layer
        f"GRANT USAGE ON SCHEMA {database}.{psa_schema} TO ROLE ETL_SERVICE_ROLE",
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {database}.{psa_schema} TO ROLE ETL_SERVICE_ROLE",
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA {database}.{psa_schema} TO ROLE ETL_SERVICE_ROLE",
        # Data Analyst Role permissions - PREP and PRES layers (read-only)
        f"GRANT USAGE ON SCHEMA {database}.{prep_schema} TO ROLE DATA_ANALYST_ROLE",
        f"GRANT SELECT ON ALL VIEWS IN SCHEMA {database}.{prep_schema} TO ROLE DATA_ANALYST_ROLE",
        f"GRANT SELECT ON FUTURE VIEWS IN SCHEMA {database}.{prep_schema} TO ROLE DATA_ANALYST_ROLE",
        f"GRANT USAGE ON SCHEMA {database}.{pres_schema} TO ROLE DATA_ANALYST_ROLE",
        f"GRANT SELECT ON ALL VIEWS IN SCHEMA {database}.{pres_schema} TO ROLE DATA_ANALYST_ROLE",
        f"GRANT SELECT ON FUTURE VIEWS IN SCHEMA {database}.{pres_schema} TO ROLE DATA_ANALYST_ROLE",
        # Data Reader Role permissions - PRES layer only (read-only)
        f"GRANT USAGE ON SCHEMA {database}.{pres_schema} TO ROLE DATA_READER_ROLE",
        f"GRANT SELECT ON ALL VIEWS IN SCHEMA {database}.{pres_schema} TO ROLE DATA_READER_ROLE",
        f"GRANT SELECT ON FUTURE VIEWS IN SCHEMA {database}.{pres_schema} TO ROLE DATA_READER_ROLE",
        # Data Engineer Role permissions - all layers with full control
        f"GRANT ALL ON SCHEMA {database}.{raw_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON ALL TABLES IN SCHEMA {database}.{raw_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON FUTURE TABLES IN SCHEMA {database}.{raw_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON SCHEMA {database}.{psa_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON ALL TABLES IN SCHEMA {database}.{psa_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON FUTURE TABLES IN SCHEMA {database}.{psa_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON SCHEMA {database}.{prep_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON ALL VIEWS IN SCHEMA {database}.{prep_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON FUTURE VIEWS IN SCHEMA {database}.{prep_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON SCHEMA {database}.{pres_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON ALL VIEWS IN SCHEMA {database}.{pres_schema} TO ROLE DATA_ENGINEER_ROLE",
        f"GRANT ALL ON FUTURE VIEWS IN SCHEMA {database}.{pres_schema} TO ROLE DATA_ENGINEER_ROLE",
    ]


# Alias for backward compatibility
get_schema_permissions = get_database_permissions
