# Knowledge Transfer: Dagster Snowflake ETL Project

## Overview

This project implements a Dagster-based ETL pipeline that loads data into Snowflake using a Kimball-style layered architecture. The system is **configuration-driven** - all databases and entities are defined in YAML, and assets/jobs are generated dynamically.

## Architecture

### Multi-Database Structure

```
Snowflake Account
├── SALES_DB
│   ├── RAW (schema)
│   │   ├── RAW_CUSTOMERS
│   │   ├── RAW_ORDERS
│   │   └── RAW_ORDER_ITEMS
│   ├── PSA (schema)
│   │   ├── PSA_CUSTOMERS
│   │   ├── PSA_ORDERS
│   │   └── PSA_ORDER_ITEMS
│   ├── PREP (schema)
│   │   ├── PREP_CUSTOMERS (view)
│   │   ├── PREP_ORDERS (view)
│   │   └── PREP_ORDER_ITEMS (view)
│   └── PRES (schema)
│       ├── CUSTOMERS (view)
│       ├── ORDERS (view)
│       └── ORDER_ITEMS (view)
├── MARKETING_DB
│   ├── RAW / PSA / PREP / PRES
│   └── ...
├── INVENTORY_DB
│   └── ...
└── FINANCE_DB
    └── ...
```

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
| **PREP** | Views showing only active records with extracted columns | Configured columns + metadata |
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
├── config/
│   └── datasets.yaml                   # Dataset configuration (THE KEY FILE)
│
├── docs/
│   └── KNOWLEDGE_TRANSFER.md           # This document
│
└── dagster_snowflake_etl/
    ├── __init__.py
    ├── definitions.py                  # Dagster entry point (loads config)
    │
    ├── config/
    │   ├── __init__.py
    │   └── datasets.py                 # Configuration schema classes
    │
    ├── resources/
    │   ├── __init__.py
    │   └── snowflake.py                # Snowflake connection resource
    │
    ├── assets/
    │   ├── __init__.py
    │   └── factory.py                  # Dynamic asset generation
    │
    └── sql/
        ├── __init__.py
        ├── schema.py                   # Schema/table/view DDL generators
        ├── governance.py               # Role/permission SQL generators
        └── etl.py                      # Merge SQL generators
```

## Configuration System

### The datasets.yaml File

This is the **single source of truth** for all databases and entities. Located at `config/datasets.yaml`.

```yaml
databases:
  - name: SALES_DB                    # Snowflake database name
    description: "Sales and order data"
    raw_schema: RAW                   # Schema names (defaults shown)
    psa_schema: PSA
    prep_schema: PREP
    pres_schema: PRES
    entities:
      - name: customers               # Entity name (used in table names)
        key_field: "customer_id"      # Field in DOC used as KEY
        description: "Customer master data"
        prep_columns:                 # Column extractions for PREP view
          - name: customer_id
            expression: "DOC:customer_id::VARCHAR"
          - name: name
            expression: "DOC:name::VARCHAR"
          - name: email
            expression: "DOC:email::VARCHAR"
```

### Generated Objects

For each entity in the config, the system generates:

| Object | Name Pattern | Example |
|--------|--------------|---------|
| RAW table | `RAW_{ENTITY}` | `SALES_DB.RAW.RAW_CUSTOMERS` |
| PSA table | `PSA_{ENTITY}` | `SALES_DB.PSA.PSA_CUSTOMERS` |
| PREP view | `PREP_{ENTITY}` | `SALES_DB.PREP.PREP_CUSTOMERS` |
| PRES view | `{ENTITY}` | `SALES_DB.PRES.CUSTOMERS` |

### Generated Assets

For each database:
- `{db_name}_schemas` - Creates RAW/PSA/PREP/PRES schemas
- `{db_name}_governance` - Grants permissions

For each entity:
- `{db_name}_{entity}_tables` - Creates tables and views
- `{db_name}_{entity}_merge` - RAW to PSA merge operation

### Generated Jobs

| Job Pattern | Description |
|-------------|-------------|
| `global_governance_job` | Creates governance roles |
| `{db_name}_setup_job` | Full database setup |
| `{db_name}_{entity}_merge_job` | Single entity merge |
| `all_merges_job` | All entity merges |
| `full_setup_job` | Complete setup |

---

## Adding a New Dataset

### Option 1: Add Entity to Existing Database

Edit `config/datasets.yaml` and add a new entity:

```yaml
databases:
  - name: SALES_DB
    entities:
      # ... existing entities ...

      - name: refunds                 # NEW ENTITY
        key_field: "refund_id"
        description: "Customer refunds"
        prep_columns:
          - name: refund_id
            expression: "DOC:refund_id::VARCHAR"
          - name: order_id
            expression: "DOC:order_id::VARCHAR"
          - name: amount
            expression: "DOC:amount::DECIMAL(18,2)"
          - name: reason
            expression: "DOC:reason::VARCHAR"
          - name: refund_date
            expression: "DOC:refund_date::DATE"
```

That's it. Restart Dagster and you'll have:
- New assets: `sales_db_refunds_tables`, `sales_db_refunds_merge`
- New job: `sales_db_refunds_merge_job`

### Option 2: Add a New Database

Edit `config/datasets.yaml` and add a new database:

```yaml
databases:
  # ... existing databases ...

  - name: HR_DB                       # NEW DATABASE
    description: "Human resources data"
    raw_schema: RAW
    psa_schema: PSA
    prep_schema: PREP
    pres_schema: PRES
    entities:
      - name: employees
        key_field: "employee_id"
        description: "Employee records"
        prep_columns:
          - name: employee_id
            expression: "DOC:employee_id::VARCHAR"
          - name: name
            expression: "DOC:name::VARCHAR"
          - name: department
            expression: "DOC:department::VARCHAR"
          - name: hire_date
            expression: "DOC:hire_date::DATE"
          - name: salary
            expression: "DOC:salary::DECIMAL(18,2)"

      - name: departments
        key_field: "department_id"
        description: "Department hierarchy"
        prep_columns:
          - name: department_id
            expression: "DOC:department_id::VARCHAR"
          - name: name
            expression: "DOC:name::VARCHAR"
          - name: manager_id
            expression: "DOC:manager_id::VARCHAR"
```

Restart Dagster and you'll have:
- New asset group: `hr_db`
- New jobs: `hr_db_setup_job`, `hr_db_employees_merge_job`, `hr_db_departments_merge_job`

### Option 3: Minimal Entity (No Column Extraction)

If you don't need column extraction in PREP (just want the raw DOC):

```yaml
- name: raw_events
  key_field: "event_id"
  description: "Raw event stream"
  # No prep_columns - PREP view will show DOC, DOC_HASH, etc.
```

---

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

### Permission Flow

1. Run `global_governance_job` once to create roles
2. Each `{db}_setup_job` grants permissions for that database

---

## Typical Workflows

### Initial Setup

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with Snowflake credentials

# 2. Install dependencies
uv sync

# 3. Start Dagster
uv run dagster dev

# 4. In Dagster UI:
#    - Run global_governance_job (creates roles)
#    - Run full_setup_job (creates all schemas/tables/views)
```

### Adding a New Data Source

1. Edit `config/datasets.yaml` (add entity or database)
2. Restart Dagster (`Ctrl+C` and `uv run dagster dev`)
3. Run the appropriate setup job:
   - New entity: `{db}_setup_job` or just materialize `{db}_{entity}_tables`
   - New database: `{db}_setup_job`

### Running ETL

```bash
# Single entity
dagster job execute -j sales_db_customers_merge_job

# All entities in a database
dagster job execute -j sales_db_setup_job

# All merges across all databases
dagster job execute -j all_merges_job
```

### Loading Data into RAW

Before running merge jobs, load data into RAW tables:

```sql
-- Example: Load JSON data into RAW
INSERT INTO SALES_DB.RAW.RAW_CUSTOMERS (DOC, KEY, SOURCE)
SELECT
    PARSE_JSON(column1),
    PARSE_JSON(column1):customer_id::VARCHAR,
    'api'
FROM VALUES
    ('{"customer_id": "C001", "name": "John Doe", "email": "john@example.com"}'),
    ('{"customer_id": "C002", "name": "Jane Smith", "email": "jane@example.com"}');
```

---

## Configuration Reference

### EntityConfig Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Entity name (used in table/view names) |
| `key_field` | Yes | JSON field used as KEY |
| `description` | No | Human-readable description |
| `prep_columns` | No | Column extractions for PREP view |

### prep_columns Entry

| Field | Description |
|-------|-------------|
| `name` | Column name in PREP view |
| `expression` | Snowflake expression to extract from DOC |

Common expressions:
```yaml
# String
expression: "DOC:field_name::VARCHAR"

# Integer
expression: "DOC:field_name::INTEGER"

# Decimal
expression: "DOC:field_name::DECIMAL(18,2)"

# Date
expression: "DOC:field_name::DATE"

# Timestamp
expression: "DOC:field_name::TIMESTAMP"

# Nested field
expression: "DOC:parent:child::VARCHAR"

# Array element
expression: "DOC:items[0]:name::VARCHAR"
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account identifier |
| `SNOWFLAKE_USER` | Yes | Username |
| `SNOWFLAKE_PASSWORD` | Yes | Password |
| `SNOWFLAKE_WAREHOUSE` | Yes | Compute warehouse |
| `SNOWFLAKE_ROLE` | Yes | Role to use |
| `DATASETS_CONFIG_PATH` | No | Override config file location |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Asset not appearing | Restart Dagster after editing config |
| Permission denied | Run `global_governance_job` then `{db}_setup_job` |
| Table already exists | Safe - uses `CREATE IF NOT EXISTS` |
| Merge returns 0 rows | Verify RAW table has data |
| Config parse error | Check YAML indentation |

---

## Best Practices

1. **One entity per data source** - Don't mix different data sources in one entity
2. **Descriptive key_field** - Use the natural business key
3. **Extract only needed columns** - Don't extract every field into PREP
4. **Test locally first** - Use Dagster UI to test before scheduling
5. **Version control config** - The datasets.yaml is your infrastructure-as-code
