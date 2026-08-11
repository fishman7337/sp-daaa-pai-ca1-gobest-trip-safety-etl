"""Database helpers used by the pipeline runner and tests."""

from __future__ import annotations

from contextlib import closing
from typing import Any

from pai_pipeline.config import DatabaseConfig
from pai_pipeline.sql import quote_identifier, sql_statement


def count_uncleaned_rows(config: DatabaseConfig, table_name: str) -> int:
    """Return rows still waiting for ETL processing.

    The import is intentionally local so offline tests and CI do not need the
    MySQL client dependency unless this integration helper is used.
    """
    import pymysql

    with closing(
        pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )
    ) as connection:
        with connection.cursor() as cursor:
            quoted_table = quote_identifier(table_name)
            # DB-API parameters cannot represent identifiers; quote_identifier
            # escapes embedded delimiters before this trusted query is assembled.
            query = sql_statement(
                "SELECT COUNT(*) FROM",
                quoted_table,
                "WHERE cleaned_date IS NULL;",
            )
            cursor.execute(query)
            row: Any = cursor.fetchone()
            return int(row[0])


def select_one(config: DatabaseConfig) -> tuple[Any, ...] | None:
    """Execute ``SELECT 1`` against a configured MySQL database."""
    import pymysql

    with closing(
        pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1;")
            return cursor.fetchone()
