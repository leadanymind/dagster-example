# Knowledge Transfer: Dagster Snowflake ETL Project

## Overview

This project implements a Dagster-based ETL pipeline that loads data into Snowflake using a Kimball-style layered architecture. The architecture enforces data quality, maintains full history via SCD Type 2, and provides clear separation between raw ingestion and consumption layers.

## Architecture

### Data Layer Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW                                        │
│                                                                          │
│   Source Data                                                            │
│       │                                                                  │
│       ▼                                                                  │
│   ┌───────┐    ┌───────┐    ┌───────┐    ┌───────┐                      │
│   │  RAW  │───▶│  PSA  │───▶│ PREP  │───▶│ PRES  │                      │
│   └───────┘    └───────┘    └───────┘    └───────┘                      │
│    Staging     Persistent    Active       Consumer                       │
│    (temp)      (history)     (current)    (public)                       │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Schema |
|-------|---------|--------|
| **RAW** | Temporary staging for incoming data | `DOC`, `KEY`, `BATCH_TS`, `SOURCE` |
| **PSA** | Persistent storage with full history (SCD Type 2) | `DOC`, `DOC_HASH`, `KEY_DOC`, `KEY_DOC_HASH`, `START_TS`, `END_TS`, `BATCH_TS`, `SOURCE` |
| **PREP** | Views showing only active records (`END_TS` year = 9999) | Same as PSA, filtered |
| **PRES** | Presentation views for consumers | `SELECT *` from PREP |

### SCD Type 2 Implementation

When data changes:
1. The existing record in PSA is "closed" by setting `END_TS` to the current timestamp
2. A new record is inserted with `END_TS = '9999-12-31'`
3. PREP/PRES views automatically reflect only the current state

## Project Structure

```
dagster-example/
├── pyproject.toml                      # Package config (uv)
├── .env.example                        # Environment template
├── schema.md                           # Layer documentation
│
├── docs/
│   └── KNOWLEDGE_TRANSFER.md           # This document
│
└── dagster_snowflake_etl/
    ├── __init__.py
    ├── definitions.py                  # Dagster entry point
    │
    ├── resources/
    │   ├── __init__.py
    │   └── snowflake.py                # Snowflake connection resource
    │
    ├── assets/
    │   ├── __init__.py                 # Asset exports
    │   ├── schema_setup.py             # DDL for RAW/PSA/PREP/PRES
    │   ├── governance.py               # Roles and permissions
    │   └── etl.py                      # RAW to PSA merge logic
    │
    ├── jobs/
    │   ├── __init__.py
    │   └── etl_jobs.py                 # Job definitions
    │
    └── sql/
        ├── __init__.py
        ├── schema.py                   # Schema DDL templates
        ├── governance.py               # Role/permission SQL
        └── etl.py                      # Merge SQL templates
```

## Key Components

### Resources (`resources/snowflake.py`)

The `SnowflakeResource` class provides:
- Connection management with context managers
- `execute_sql()` for single statements
- `execute_sql_multi()` for multiple statements
- Schema name configuration for all layers

### Assets

Assets are the core building blocks. Each asset:
- Declares its dependencies via `deps=[]`
- Belongs to a group for job selection
- Returns metadata for observability

### Jobs (`jobs/etl_jobs.py`)

| Job | Purpose |
|-----|---------|
| `schema_setup_job` | Creates all schema layers |
| `governance_setup_job` | Creates roles and grants permissions |
| `full_setup_job` | Complete initial setup |
| `etl_pipeline_job` | Runs ETL (RAW → PSA merge) |

## Data Governance

### Roles

| Role | Access |
|------|--------|
| `ETL_SERVICE_ROLE` | CRUD on RAW and PSA |
| `DATA_ANALYST_ROLE` | SELECT on PREP and PRES |
| `DATA_READER_ROLE` | SELECT on PRES only |
| `DATA_ENGINEER_ROLE` | ALL on all schemas |

### Role Hierarchy

```
DATA_READER_ROLE
       ▲
DATA_ANALYST_ROLE
       ▲
DATA_ENGINEER_ROLE ◀── ETL_SERVICE_ROLE
```

---

## Adding a New ETL Pipeline

This section describes how to add a new data source/entity to the pipeline.

### Step 1: Define the RAW and PSA Tables

Add table definitions to `sql/schema.py`:

```python
def get_raw_customers_ddl(database: str, schema: str) -> list[str]:
    """DDL for customers RAW table."""
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.RAW_CUSTOMERS (
            DOC VARIANT NOT NULL,
            KEY VARCHAR NOT NULL,
            BATCH_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            SOURCE VARCHAR NOT NULL
        )
        """
    ]


def get_psa_customers_ddl(database: str, schema: str) -> list[str]:
    """DDL for customers PSA table."""
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.PSA_CUSTOMERS (
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
    ]
```

### Step 2: Create PREP and PRES Views

Add view definitions to `sql/schema.py`:

```python
def get_prep_customers_ddl(database: str, prep_schema: str, psa_schema: str) -> list[str]:
    """DDL for customers PREP view."""
    return [
        f"""
        CREATE OR REPLACE VIEW {database}.{prep_schema}.PREP_CUSTOMERS AS
        SELECT
            DOC:customer_id::VARCHAR AS customer_id,
            DOC:name::VARCHAR AS name,
            DOC:email::VARCHAR AS email,
            DOC:created_at::TIMESTAMP AS created_at,
            START_TS,
            END_TS,
            BATCH_TS,
            SOURCE
        FROM {database}.{psa_schema}.PSA_CUSTOMERS
        WHERE YEAR(END_TS) = 9999
        """
    ]


def get_pres_customers_ddl(database: str, pres_schema: str, prep_schema: str) -> list[str]:
    """DDL for customers PRES view."""
    return [
        f"""
        CREATE OR REPLACE VIEW {database}.{pres_schema}.CUSTOMERS AS
        SELECT * FROM {database}.{prep_schema}.PREP_CUSTOMERS
        """
    ]
```

### Step 3: Create the Schema Asset

Create a new file `assets/customers.py`:

```python
"""Assets for customers ETL pipeline."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue

from dagster_snowflake_etl.resources.snowflake import SnowflakeResource
from dagster_snowflake_etl.sql.schema import (
    get_raw_customers_ddl,
    get_psa_customers_ddl,
    get_prep_customers_ddl,
    get_pres_customers_ddl,
)


@asset(
    group_name="customers",
    deps=["raw_schema", "psa_schema", "prep_schema", "pres_schema"],
    description="Creates customers tables and views across all layers",
)
def customers_schema(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Create customers tables in RAW, PSA and views in PREP, PRES."""
    database = snowflake.database

    ddl_statements = (
        get_raw_customers_ddl(database, snowflake.raw_schema) +
        get_psa_customers_ddl(database, snowflake.psa_schema) +
        get_prep_customers_ddl(database, snowflake.prep_schema, snowflake.psa_schema) +
        get_pres_customers_ddl(database, snowflake.pres_schema, snowflake.prep_schema)
    )

    context.log.info(f"Creating customers schema objects...")
    snowflake.execute_sql_multi(ddl_statements)

    return Output(
        value={"entity": "customers", "statements": len(ddl_statements)},
        metadata={"statements_executed": MetadataValue.int(len(ddl_statements))},
    )
```

### Step 4: Create the ETL Merge Asset

Add to `assets/customers.py`:

```python
from dagster_snowflake_etl.sql.etl import (
    get_raw_to_psa_merge_sql,
    get_close_changed_records_sql,
)


@asset(
    group_name="customers",
    deps=["customers_schema"],
    description="Merges customers data from RAW to PSA with SCD Type 2",
)
def customers_merge(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Merge customers from RAW to PSA."""
    database = snowflake.database
    raw_schema = snowflake.raw_schema
    psa_schema = snowflake.psa_schema

    # Close changed records
    close_sql = get_close_changed_records_sql(
        database, raw_schema, psa_schema,
        raw_table="RAW_CUSTOMERS",
        psa_table="PSA_CUSTOMERS",
    )
    snowflake.execute_sql(close_sql)

    # Merge new/changed records
    merge_sql = get_raw_to_psa_merge_sql(
        database, raw_schema, psa_schema,
        raw_table="RAW_CUSTOMERS",
        psa_table="PSA_CUSTOMERS",
    )
    result = snowflake.execute_sql(merge_sql)
    records_merged = result[0][0] if result else 0

    context.log.info(f"Merged {records_merged} customer records")

    return Output(
        value={"records_merged": records_merged},
        metadata={"records_merged": MetadataValue.int(records_merged)},
    )
```

### Step 5: Create an Ingestion Asset (Optional)

If you need to ingest data from an external source:

```python
@asset(
    group_name="customers",
    deps=["customers_schema"],
    description="Loads customers from external API into RAW layer",
)
def customers_ingest(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[dict]:
    """Ingest customers from external source into RAW."""
    import json

    # Fetch data from your source (API, file, etc.)
    customers = fetch_customers_from_api()  # Your implementation

    # Insert into RAW
    insert_sql = f"""
        INSERT INTO {snowflake.database}.{snowflake.raw_schema}.RAW_CUSTOMERS
        (DOC, KEY, SOURCE)
        SELECT
            PARSE_JSON(column1),
            PARSE_JSON(column1):customer_id::VARCHAR,
            'api'
        FROM VALUES {','.join(f"('{json.dumps(c)}')" for c in customers)}
    """
    snowflake.execute_sql(insert_sql)

    return Output(
        value={"records_ingested": len(customers)},
        metadata={"records_ingested": MetadataValue.int(len(customers))},
    )
```

### Step 6: Register Assets and Create Job

Update `assets/__init__.py`:

```python
from .customers import (
    customers_schema,
    customers_merge,
    # customers_ingest,  # if applicable
)

__all__ = [
    # ... existing exports ...
    "customers_schema",
    "customers_merge",
]
```

Add a job in `jobs/etl_jobs.py`:

```python
customers_etl_job = define_asset_job(
    name="customers_etl_job",
    selection=AssetSelection.groups("customers"),
    description="Full customers ETL: schema setup and merge",
)
```

Update `definitions.py` to include the new assets and job.

### Step 7: Grant Permissions (Optional)

If the new tables need specific permissions, add grants to `sql/governance.py` and re-run the governance job.

---

## Best Practices

### Naming Conventions

| Object | Convention | Example |
|--------|------------|---------|
| RAW tables | `RAW_{ENTITY}` | `RAW_CUSTOMERS` |
| PSA tables | `PSA_{ENTITY}` | `PSA_CUSTOMERS` |
| PREP views | `PREP_{ENTITY}` | `PREP_CUSTOMERS` |
| PRES views | `{ENTITY}` (clean name) | `CUSTOMERS` |
| Asset groups | Entity name (lowercase) | `customers` |
| Jobs | `{entity}_etl_job` | `customers_etl_job` |

### Asset Dependencies

```
raw_schema ──▶ psa_schema ──▶ prep_schema ──▶ pres_schema
                                    │
                                    ▼
                          {entity}_schema
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             {entity}_ingest  {entity}_merge  {entity}_transform
```

### Testing

1. **Local testing**: Use `dagster dev` and trigger jobs manually
2. **Dry run**: Add a `dry_run` config option to SQL execution
3. **Unit tests**: Mock the `SnowflakeResource` for SQL generation tests

### Error Handling

- Always log SQL statements at DEBUG level before execution
- Use transactions for multi-statement operations when possible
- Return meaningful metadata for observability in Dagster UI

---

## Environment Setup

```bash
# Clone and install
git clone https://github.com/leadanymind/dagster-example.git
cd dagster-example
cp .env.example .env
# Edit .env with Snowflake credentials
uv sync

# Run locally
uv run dagster dev

# First-time setup (run in Dagster UI)
# 1. Run full_setup_job (creates schemas + governance)
# 2. Run individual ETL jobs as needed
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Permission denied | Ensure your Snowflake role has required grants |
| Table already exists | Safe - using `CREATE IF NOT EXISTS` |
| View creation fails | Check that underlying PSA table exists |
| Merge returns 0 rows | Verify RAW table has data to merge |
