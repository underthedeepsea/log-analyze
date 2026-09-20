from __future__ import annotations

import sqlite3

import pytest

from logrisk.database import SQLiteDatabase


def test_owned_sqlite_connection_closes_after_normal_and_exceptional_context(tmp_path):
    database = SQLiteDatabase(tmp_path / "db.sqlite3")

    connection = database.connect()
    with connection:
        connection.execute("SELECT 1")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")

    failed = database.connect()
    with pytest.raises(RuntimeError, match="boom"):
        with failed:
            raise RuntimeError("boom")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        failed.execute("SELECT 1")


def test_transaction_closes_owned_connection(tmp_path):
    database = SQLiteDatabase(tmp_path / "db.sqlite3")
    with database.transaction() as connection:
        connection.execute("CREATE TABLE example(id INTEGER PRIMARY KEY)")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")
