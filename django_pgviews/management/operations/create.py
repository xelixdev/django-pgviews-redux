from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from django.db import transaction
from django.db.backends.postgresql.base import DatabaseWrapper

from django_pgviews.compat import ProgrammingError
from django_pgviews.management.operations._utils import _make_where, _schema_and_name

if TYPE_CHECKING:
    from django_pgviews.view import ViewSQL


@transaction.atomic()
def create_view(
    connection: DatabaseWrapper, view_name: str, view_query: ViewSQL, update: bool = True, force: bool = False
) -> Literal["EXISTS", "UPDATED", "CREATED", "FORCE_REQUIRED"]:
    """
    Create a named view on a connection.

    Returns True if a new view was created (or an existing one updated), or
    False if nothing was done.

    If ``update`` is True (default), attempt to update an existing view. If the
    existing view's schema is incompatible with the new definition, ``force``
    (default: False) controls whether or not to drop the old view and create
    the new one.
    """

    vschema, vname = _schema_and_name(connection, view_name)

    cursor_wrapper = connection.cursor()
    cursor = cursor_wrapper.cursor
    try:
        force_required = False
        # Determine if view already exists.
        view_exists_where, view_exists_params = _make_where(table_schema=vschema, table_name=vname)
        cursor.execute(
            f"SELECT COUNT(*) FROM information_schema.views WHERE {view_exists_where};",
            view_exists_params,
        )
        view_exists = cursor.fetchone()[0] > 0
        if view_exists and not update:
            return "EXISTS"
        elif view_exists:
            # Detect schema conflict by copying the original view, attempting to
            # update this copy, and detecting errors.
            cursor.execute(f"CREATE TEMPORARY VIEW check_conflict AS SELECT * FROM {view_name};")
            try:
                with transaction.atomic():
                    cursor.execute(
                        f"CREATE OR REPLACE TEMPORARY VIEW check_conflict AS {view_query.query};",
                        view_query.params,
                    )
            except ProgrammingError:
                force_required = True
            finally:
                cursor.execute("DROP VIEW IF EXISTS check_conflict;")

        if not force_required:
            cursor.execute(f"CREATE OR REPLACE VIEW {view_name} AS {view_query.query};", view_query.params)
            ret = view_exists and "UPDATED" or "CREATED"
        elif force:
            cursor.execute(f"DROP VIEW IF EXISTS {view_name} CASCADE;")
            cursor.execute(f"CREATE VIEW {view_name} AS {view_query.query};", view_query.params)
            ret = "FORCED"
        else:
            ret = "FORCE_REQUIRED"

        return ret
    finally:
        cursor_wrapper.close()
