"""SQL templates for ETL operations."""


def get_raw_to_psa_merge_sql(
    database: str,
    raw_schema: str,
    psa_schema: str,
    raw_table: str = "RAW_DATA",
    psa_table: str = "PSA_DATA",
) -> str:
    """Generate MERGE SQL for RAW to PSA layer.

    This implements SCD Type 2 logic:
    1. Close existing records (set END_TS) when the document hash changes
    2. Insert new records when:
       - The key doesn't exist in PSA
       - The document has changed (different DOC_HASH)

    The KEY from RAW becomes KEY_DOC in PSA (stored as variant for flexibility).
    """
    return f"""
    MERGE INTO {database}.{psa_schema}.{psa_table} AS target
    USING (
        SELECT
            DOC,
            SHA2_BINARY(DOC::VARCHAR) AS DOC_HASH,
            TO_VARIANT(KEY) AS KEY_DOC,
            SHA2_BINARY(KEY) AS KEY_DOC_HASH,
            BATCH_TS,
            SOURCE
        FROM {database}.{raw_schema}.{raw_table}
    ) AS source
    ON target.KEY_DOC_HASH = source.KEY_DOC_HASH
       AND YEAR(target.END_TS) = 9999
       AND target.DOC_HASH = source.DOC_HASH
    WHEN NOT MATCHED THEN
        INSERT (DOC, DOC_HASH, KEY_DOC, KEY_DOC_HASH, START_TS, END_TS, BATCH_TS, SOURCE)
        VALUES (
            source.DOC,
            source.DOC_HASH,
            source.KEY_DOC,
            source.KEY_DOC_HASH,
            CURRENT_TIMESTAMP(),
            '9999-12-31'::TIMESTAMP_LTZ,
            source.BATCH_TS,
            source.SOURCE
        )
    """


def get_close_changed_records_sql(
    database: str,
    raw_schema: str,
    psa_schema: str,
    raw_table: str = "RAW_DATA",
    psa_table: str = "PSA_DATA",
) -> str:
    """Generate SQL to close records in PSA when document has changed.

    This runs before the merge to implement SCD Type 2:
    - Find records where the key exists but the document hash is different
    - Set END_TS to current timestamp for those records
    """
    return f"""
    UPDATE {database}.{psa_schema}.{psa_table} AS target
    SET END_TS = CURRENT_TIMESTAMP()
    WHERE YEAR(target.END_TS) = 9999
    AND EXISTS (
        SELECT 1
        FROM {database}.{raw_schema}.{raw_table} AS source
        WHERE SHA2_BINARY(source.KEY) = target.KEY_DOC_HASH
          AND SHA2_BINARY(source.DOC::VARCHAR) != target.DOC_HASH
    )
    """


def get_truncate_raw_sql(
    database: str,
    raw_schema: str,
    raw_table: str = "RAW_DATA",
) -> str:
    """Generate SQL to truncate RAW table after successful merge."""
    return f"TRUNCATE TABLE {database}.{raw_schema}.{raw_table}"
