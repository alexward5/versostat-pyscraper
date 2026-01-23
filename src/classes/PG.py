from typing import Any, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import sql


class PG:
    def __init__(self, dbname: str, user: str):
        self.conn = psycopg2.connect(f"dbname={dbname} user={user}")

    def __del__(self):
        self.conn.close()

    def drop_schema(self, schema_name: str):
        with self.conn.cursor() as cur:
            cur.execute(  # type: ignore
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name)),
            )

            self.conn.commit()
            print(f"Dropped schema: {schema_name}")

    def create_schema(self, schema_name: str):
        with self.conn.cursor() as cur:
            cur.execute(  # type: ignore
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)),
            )

            self.conn.commit()
            print(f"Created schema: {schema_name}")

    def drop_table(self, schema: str, table_name: str):
        with self.conn.cursor() as cur:
            cur.execute(  # type: ignore
                sql.SQL("DROP TABLE IF EXISTS {schema}.{table_name} CASCADE").format(
                    schema=sql.Identifier(schema),
                    table_name=sql.Identifier(table_name),
                ),
            )

            self.conn.commit()
            print(f"Dropped table: {table_name}")

    def create_table(self, schema: str, table_name: str, columns: list[Any]):
        with self.conn.cursor() as cur:
            cur.execute(  # type: ignore
                sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({columns})").format(
                    schema=sql.Identifier(schema),
                    table_name=sql.Identifier(table_name),
                    columns=sql.SQL(",".join(columns)),
                ),
            )

            self.conn.commit()
            print(f"Created table: {table_name}")

    def insert_row(
        self,
        schema: str,
        table_name: str,
        column_names: list[Any],
        row_values: list[Any],
        update_on: Optional[str] = None,
    ):
        with self.conn.cursor() as cur:
            column_names_joined = ",".join(column_names)
            row_values_joined = ", ".join(f"'{val}'" for val in row_values)

            if update_on:
                on_conflict = f"ON CONFLICT ({update_on}) DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in column_names if col != update_on])}"  # noqa

            else:
                on_conflict = "ON CONFLICT DO NOTHING"

            cur.execute(  # type: ignore
                sql.SQL(
                    "INSERT INTO {schema}.{table_name}({column_names}) VALUES ({row_values}) {on_conflict}"  # noqa
                ).format(
                    schema=sql.Identifier(schema),
                    table_name=sql.Identifier(table_name),
                    column_names=sql.SQL(column_names_joined),
                    row_values=sql.SQL(row_values_joined),  # type: ignore
                    on_conflict=sql.SQL(on_conflict),
                ),
            )

            self.conn.commit()

    def query_table(
        self,
        schema: str,
        table_name: str,
        columns: Optional[list[str]] = None,
        where_clause: Optional[str] = "",
    ) -> list[Any]:
        query_columns = "*"
        if columns:
            query_columns = ",".join(columns)

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(  # type: ignore
                sql.SQL("SELECT {query_columns} FROM {schema}.{table_name} {where_clause}").format(
                    query_columns=sql.SQL(query_columns),
                    schema=sql.Identifier(schema),
                    table_name=sql.Identifier(table_name),
                    where_clause=sql.SQL(where_clause),  # type: ignore
                ),
            )

            rows = cur.fetchall()

        return rows

    def create_view(
        self,
        schema: str,
        view_name: str,
        view_query: Any,
    ) -> None:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(  # type: ignore
                sql.SQL("DROP VIEW IF EXISTS {schema}.{view_name} CASCADE").format(
                    schema=sql.Identifier(schema),
                    view_name=sql.Identifier(view_name),
                ),
            )

            cur.execute(  # type: ignore
                sql.SQL("CREATE OR REPLACE VIEW {schema}.{view_name} AS {view_query}").format(
                    schema=sql.Identifier(schema),
                    view_name=sql.Identifier(view_name),
                    view_query=sql.SQL(view_query),
                ),
            )

            self.conn.commit()

    def create_materialized_view(
        self,
        schema: str,
        view_name: str,
        view_query: Any,
    ) -> None:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(  # type: ignore
                sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {schema}.{view_name} CASCADE").format(
                    schema=sql.Identifier(schema),
                    view_name=sql.Identifier(view_name),
                ),
            )

            cur.execute(  # type: ignore
                sql.SQL("CREATE MATERIALIZED VIEW {schema}.{view_name} AS {view_query}").format(
                    schema=sql.Identifier(schema),
                    view_name=sql.Identifier(view_name),
                    view_query=sql.SQL(view_query),
                ),
            )

            self.conn.commit()

    def create_index(
        self,
        schema: str,
        table_name: str,
        index_name: str,
        columns: list[str],
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(  # type: ignore
                sql.SQL("DROP INDEX IF EXISTS {schema}.{index_name}").format(
                    schema=sql.Identifier(schema),
                    index_name=sql.Identifier(index_name),
                ),
            )

            columns_identifiers = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

            cur.execute(  # type: ignore
                sql.SQL("CREATE INDEX {index_name} ON {schema}.{table_name} ({columns})").format(
                    index_name=sql.Identifier(index_name),
                    schema=sql.Identifier(schema),
                    table_name=sql.Identifier(table_name),
                    columns=columns_identifiers,
                ),
            )

            self.conn.commit()
            print(f"Created index: {index_name}")
