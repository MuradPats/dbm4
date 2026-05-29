"""Trino + Iceberg adapter.

This file implements the TableEngine interface for Trino, talking to the
iceberg_datalake catalog on MinIO.
"""
from __future__ import annotations

import csv
import os
from pathlib import Path
import trino

from internal_etl_package.engines.base import TableEngine


class TrinoIcebergEngine(TableEngine):
    """Production TableEngine backed by Trino + Iceberg."""

    def __init__(self, trino_conn=None):
        self._trino = trino_conn

    def _ensure_connection(self):
        if self._trino is None:
            host = os.environ.get("TRINO_HOST", "trino")
            port = int(os.environ.get("TRINO_PORT", "8080"))
            user = os.environ.get("TRINO_USER", "airflow")
            catalog = os.environ.get("TRINO_CATALOG", "iceberg_datalake")
            self._trino = trino.dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
            )
        return self._trino

    def current_snapshot_id(self, table: str) -> int:
        self._ensure_connection()
        schema, tbl = table.split(".", 1)
        cur = self._trino.cursor()
        try:
            cur.execute(
                f'SELECT snapshot_id FROM iceberg_datalake.{schema}."{tbl}$snapshots" '
                f'ORDER BY committed_at DESC LIMIT 1'
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"No snapshots found for table {table}")
            return int(row[0])
        finally:
            cur.close()

    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> None:
        self._ensure_connection()
        schema, tbl = table.split(".", 1)
        cur = self._trino.cursor()
        try:
            cur.execute(
                f"CALL iceberg_datalake.system.rollback_to_snapshot("
                f"'{schema}', '{tbl}', {int(snapshot_id)})"
            )
            cur.fetchall()
        finally:
            cur.close()

    def merge(self, table: str, source_path: str, primary_key: str) -> int:
        self._ensure_connection()
        schema, tbl = table.split(".", 1)

        # 1. Read all CSV files from source_path
        csv_files = list(Path(source_path).glob("*.csv"))
        if not csv_files:
            # Check if source_path itself is a csv file
            if Path(source_path).is_file() and source_path.suffix == ".csv":
                csv_files = [Path(source_path)]
            else:
                raise FileNotFoundError(f"No CSV files found in {source_path}")

        # Parse CSVs
        headers = []
        rows = []
        for csv_file in csv_files:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                file_headers = next(reader, None)
                if not file_headers:
                    continue
                if not headers:
                    headers = file_headers
                for row in reader:
                    rows.append(row)

        if not headers:
            raise ValueError(f"CSV file is empty or missing headers at {source_path}")

        # 2. Infer Types
        col_types = []
        for i, col in enumerate(headers):
            # Sample first few rows to infer type
            inferred = "VARCHAR"
            for r in rows[:10]:
                if len(r) > i and r[i]:
                    inferred = self._infer_type(r[i], col)
                    break
            col_types.append(inferred)

        # 3. Create target and staging tables
        cur = self._trino.cursor()
        try:
            # Create schema
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg_datalake.{schema}")
            cur.fetchall()

            # Target table columns DDL
            cols_ddl = ", ".join(f"{col} {t}" for col, t in zip(headers, col_types))

            # Target table
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS iceberg_datalake.{table} ({cols_ddl})"
            )
            cur.fetchall()

            # Staging table
            staging_table = f"iceberg_datalake.{schema}.{tbl}_staging"
            cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
            cur.fetchall()
            cur.execute(f"CREATE TABLE {staging_table} ({cols_ddl})")
            cur.fetchall()

            # 4. Insert rows into staging (optimized using batch inserts)
            if rows:
                batch_size = 500
                for start_idx in range(0, len(rows), batch_size):
                    batch = rows[start_idx : start_idx + batch_size]
                    value_rows = []
                    for row in batch:
                        formatted_vals = [
                            self._format_val(val, t) for val, t in zip(row, col_types)
                        ]
                        value_rows.append(f"({', '.join(formatted_vals)})")
                    
                    values_str = ", ".join(value_rows)
                    cur.execute(f"INSERT INTO {staging_table} VALUES {values_str}")
                    cur.fetchall()

            # 5. MERGE INTO target table
            update_sets = ", ".join(f"{col} = source.{col}" for col in headers if col != primary_key)
            cols_str = ", ".join(headers)
            source_cols_str = ", ".join(f"source.{col}" for col in headers)

            merge_sql = f"""
                MERGE INTO iceberg_datalake.{table} AS target
                USING {staging_table} AS source
                ON target.{primary_key} = source.{primary_key}
            """
            if update_sets:
                merge_sql += f"\nWHEN MATCHED THEN UPDATE SET {update_sets}"
            merge_sql += f"\nWHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({source_cols_str})"

            cur.execute(merge_sql)
            cur.fetchall()

            # 6. Drop staging
            cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
            cur.fetchall()

            return self.current_snapshot_id(table)
        finally:
            cur.close()

    def count_duplicates(self, table: str, primary_key: str) -> int:
        self._ensure_connection()
        cur = self._trino.cursor()
        try:
            cur.execute(
                f"SELECT COUNT(*) AS dup_keys FROM ("
                f"    SELECT {primary_key}"
                f"    FROM iceberg_datalake.{table}"
                f"    GROUP BY {primary_key}"
                f"    HAVING COUNT(*) > 1"
                f")"
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()

    def row_count(self, table: str) -> int:
        self._ensure_connection()
        cur = self._trino.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM iceberg_datalake.{table}")
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()

    def null_rate(self, table: str, column: str) -> float:
        self._ensure_connection()
        cur = self._trino.cursor()
        try:
            cur.execute(
                f"SELECT "
                f"  CAST(SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS DOUBLE) "
                f"  / NULLIF(COUNT(*), 0) "
                f"FROM iceberg_datalake.{table}"
            )
            row = cur.fetchone()
            rate = row[0]
            return float(rate) if rate is not None else 0.0
        finally:
            cur.close()

    def _infer_type(self, val: str, col_name: str) -> str:
        if not val:
            return "VARCHAR"
        if col_name.endswith("_id") or col_name == "id":
            try:
                int(val)
                return "BIGINT"
            except ValueError:
                pass
        if "_" in val or "-" in val or ":" in val:
            if len(val) >= 10 and (val[4] == '-' or val[7] == '-'):
                return "TIMESTAMP"
        try:
            int(val)
            return "BIGINT"
        except ValueError:
            pass
        try:
            float(val)
            return "DOUBLE"
        except ValueError:
            pass
        return "VARCHAR"

    def _format_val(self, val: str, col_type: str) -> str:
        if not val or val.upper() == "NULL" or val == "":
            return "NULL"
        if col_type in ("BIGINT", "DOUBLE"):
            try:
                float(val)
                return val
            except ValueError:
                return "NULL"
        if col_type == "TIMESTAMP":
            escaped = val.replace("'", "''")
            return f"TIMESTAMP '{escaped}'"
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
