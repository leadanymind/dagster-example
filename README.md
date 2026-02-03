# Dagster Snowflake ETL

Dagster ETL pipeline for Snowflake with Kimball-style layered architecture.

## Setup

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
uv sync
uv run dagster dev
```

## Architecture

- **RAW**: Staging area for incoming data
- **PSA**: Persistent Staging Area with SCD Type 2 history
- **PREP**: Views of active records (END_TS year = 9999)
- **PRES**: Presentation layer for consumers
