import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv(".env.local")

logger = logging.getLogger(__name__)


class PostgresClient:
    def __init__(self) -> None:
        self.dbname = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")

        if not self.dbname or not self.user or not self.host or not self.port:
            raise ValueError("Database credentials not found in environment variables")

        self.conn: Any = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            host=self.host,
            port=self.port,
        )

    def close(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.close()

    @contextmanager
    def _cursor(self, dict_cursor: bool = False) -> Iterator[Any]:
        """Context manager for cursor with automatic commit/rollback."""
        cursor_factory = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur: Any = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def schema_exists(self, schema_name: str) -> bool:
        with self._cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)",
                (schema_name,),
            )
            return bool(cur.fetchone()[0])

    def table_exists(self, schema: str, table_name: str) -> bool:
        with self._cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
                (schema, table_name),
            )
            return bool(cur.fetchone()[0])

    def drop_schema(self, schema_name: str) -> None:
        if not self.schema_exists(schema_name):
            return

        with self._cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name))
            )
        logger.info("Dropped schema: %s", schema_name)

    def create_schema(self, schema_name: str) -> None:
        if self.schema_exists(schema_name):
            return

        with self._cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
            )
        logger.info("Created schema: %s", schema_name)

    def drop_table(self, schema: str, table_name: str) -> None:
        if not self.table_exists(schema, table_name):
            return

        with self._cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                )
            )
        logger.info("Dropped table: %s.%s", schema, table_name)

    def create_table(self, schema: str, table_name: str, columns: list[str]) -> None:
        if self.table_exists(schema, table_name):
            return

        with self._cursor() as cur:
            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(", ".join(columns)),
                )
            )
        logger.info("Created table: %s.%s", schema, table_name)

    def insert_row(
        self,
        schema: str,
        table_name: str,
        column_names: list[str],
        row_values: list[Any],
        update_on: Optional[str] = None,
    ) -> None:
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in row_values)
        columns = sql.SQL(", ").join(sql.Identifier(col) for col in column_names)

        if update_on:
            update_cols = [col for col in column_names if col != update_on]
            on_conflict = sql.SQL("ON CONFLICT ({}) DO UPDATE SET {}").format(
                sql.Identifier(update_on),
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                    for col in update_cols
                ),
            )
        else:
            on_conflict = sql.SQL("ON CONFLICT DO NOTHING")

        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) {}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            columns,
            placeholders,
            on_conflict,
        )

        with self._cursor() as cur:
            cur.execute(query, row_values)

    def query_table(
        self,
        schema: str,
        table_name: str,
        columns: Optional[list[str]] = None,
        where_clause: Optional[str] = None,
        where_params: Optional[list[Any]] = None,
    ) -> list[dict[str, Any]]:
        if columns:
            cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        else:
            cols = sql.SQL("*")

        query = sql.SQL("SELECT {} FROM {}.{}").format(
            cols,
            sql.Identifier(schema),
            sql.Identifier(table_name),
        )

        if where_clause:
            query = sql.SQL("{} WHERE {}").format(query, sql.SQL(where_clause))

        with self._cursor(dict_cursor=True) as cur:
            cur.execute(query, where_params or [])
            result: list[dict[str, Any]] = cur.fetchall()
            return result

    def create_view(self, schema: str, view_name: str, view_query: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("DROP VIEW IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(schema),
                    sql.Identifier(view_name),
                )
            )
            cur.execute(
                sql.SQL("CREATE VIEW {}.{} AS {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(view_name),
                    sql.SQL(view_query),
                )
            )
        logger.info("Created view: %s.%s", schema, view_name)

    def create_materialized_view(self, schema: str, view_name: str, view_query: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(schema),
                    sql.Identifier(view_name),
                )
            )
            cur.execute(
                sql.SQL("CREATE MATERIALIZED VIEW {}.{} AS {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(view_name),
                    sql.SQL(view_query),
                )
            )
        logger.info("Created materialized view: %s.%s", schema, view_name)

    def create_index(
        self,
        schema: str,
        table_name: str,
        index_name: str,
        columns: list[str],
    ) -> None:
        columns_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

        with self._cursor() as cur:
            cur.execute(
                sql.SQL("DROP INDEX IF EXISTS {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(index_name),
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX {} ON {}.{} ({})").format(
                    sql.Identifier(index_name),
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    columns_sql,
                )
            )
        logger.info("Created index: %s on %s.%s", index_name, schema, table_name)
