"""Snowflake resource for Dagster."""

from contextlib import contextmanager
from typing import Iterator, Any

from dagster import ConfigurableResource
import snowflake.connector
from snowflake.connector import SnowflakeConnection


class SnowflakeResource(ConfigurableResource):
    """Configurable Snowflake resource for Dagster pipelines.

    This resource manages Snowflake connections and provides methods for
    executing SQL statements. It supports multi-database operations where
    database names are specified in the SQL statements themselves.
    """

    account: str
    user: str
    password: str
    warehouse: str
    role: str

    @contextmanager
    def get_connection(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> Iterator[SnowflakeConnection]:
        """Get a Snowflake connection context manager.

        Args:
            database: Optional database to connect to
            schema: Optional schema to connect to
        """
        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=database,
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
        database: str | None = None,
        schema: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[tuple]:
        """Execute SQL and return results.

        Args:
            sql: SQL statement to execute
            database: Optional database context
            schema: Optional schema context
            params: Optional query parameters
        """
        with self.get_connection(database, schema) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or {})
                return cursor.fetchall()
            finally:
                cursor.close()

    def execute_sql_multi(
        self,
        statements: list[str],
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        """Execute multiple SQL statements.

        Args:
            statements: List of SQL statements to execute
            database: Optional database context
            schema: Optional schema context
        """
        with self.get_connection(database, schema) as conn:
            cursor = conn.cursor()
            try:
                for stmt in statements:
                    if stmt.strip():
                        cursor.execute(stmt)
            finally:
                cursor.close()
