"""Snowflake resource for Dagster."""

from contextlib import contextmanager
from typing import Iterator, Any

from dagster import ConfigurableResource, InitResourceContext
import snowflake.connector
from snowflake.connector import SnowflakeConnection


class SnowflakeResource(ConfigurableResource):
    """Configurable Snowflake resource for Dagster pipelines."""

    account: str
    user: str
    password: str
    warehouse: str
    database: str
    role: str
    raw_schema: str = "RAW"
    psa_schema: str = "PSA"
    prep_schema: str = "PREP"
    pres_schema: str = "PRES"

    @contextmanager
    def get_connection(self, schema: str | None = None) -> Iterator[SnowflakeConnection]:
        """Get a Snowflake connection context manager."""
        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=schema,
            role=self.role,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_sql(
        self,
        sql: str,
        schema: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[tuple]:
        """Execute SQL and return results."""
        with self.get_connection(schema) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or {})
                return cursor.fetchall()
            finally:
                cursor.close()

    def execute_sql_multi(
        self,
        statements: list[str],
        schema: str | None = None,
    ) -> None:
        """Execute multiple SQL statements."""
        with self.get_connection(schema) as conn:
            cursor = conn.cursor()
            try:
                for stmt in statements:
                    if stmt.strip():
                        cursor.execute(stmt)
            finally:
                cursor.close()


def snowflake_resource(context: InitResourceContext) -> SnowflakeResource:
    """Factory function for SnowflakeResource."""
    return SnowflakeResource(
        account=context.resource_config["account"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        warehouse=context.resource_config["warehouse"],
        database=context.resource_config["database"],
        role=context.resource_config["role"],
        raw_schema=context.resource_config.get("raw_schema", "RAW"),
        psa_schema=context.resource_config.get("psa_schema", "PSA"),
        prep_schema=context.resource_config.get("prep_schema", "PREP"),
        pres_schema=context.resource_config.get("pres_schema", "PRES"),
    )
