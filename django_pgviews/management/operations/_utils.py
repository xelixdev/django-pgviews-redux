from __future__ import annotations

from typing import Any

from django.db.backends.postgresql.base import DatabaseWrapper


def _schema_and_name(connection: DatabaseWrapper, view_name: str) -> tuple[str | None, str]:
    if "." in view_name:
        return view_name.split(".", 1)
    else:
        try:
            schema_name = connection.schema_name
        except AttributeError:
            schema_name = None

        return schema_name, view_name


def _make_where(**kwargs: Any) -> tuple[str, list[Any]]:
    where_fragments = []
    params = []

    for key, value in kwargs.items():
        if value is None:
            # skip key if value is not specified
            continue

        if isinstance(value, list | tuple):
            in_fragment = ", ".join("%s" for _ in range(len(value)))
            where_fragments.append(f"{key} IN ({in_fragment})")
            params.extend(list(value))
        else:
            where_fragments.append(f"{key} = %s")
            params.append(value)
    where_fragment = " AND ".join(where_fragments)
    return where_fragment, params
