from __future__ import annotations

from typing import Literal

from django.db.backends.postgresql.base import DatabaseWrapper


def clear_view(connection: DatabaseWrapper, view_name: str, materialized: bool = False) -> Literal["DROPPED"]:
    """
    Remove a named view on connection.
    """
    cursor_wrapper = connection.cursor()
    cursor = cursor_wrapper.cursor
    try:
        if materialized:
            cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
        else:
            cursor.execute(f"DROP VIEW IF EXISTS {view_name} CASCADE")
    finally:
        cursor_wrapper.close()
    return "DROPPED"
