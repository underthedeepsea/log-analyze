from __future__ import annotations

from logrisk.database import PostgresCursor


class DriverCursor:
    rowcount = 2

    def __init__(self):
        self.rows = iter(({"id": 1}, {"id": 2}))
        self.fetchall_called = False

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.rows)

    def fetchone(self):
        return next(self.rows, None)

    def fetchall(self):
        self.fetchall_called = True
        return list(self.rows)


def test_postgres_cursor_wraps_rows_lazily_without_fetchall():
    driver = DriverCursor()

    assert next(iter(PostgresCursor(driver)))["id"] == 1
    assert driver.fetchall_called is False
