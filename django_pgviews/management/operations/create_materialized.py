from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

from django.db import transaction
from django.db.backends.postgresql.base import DatabaseWrapper
from django.db.backends.postgresql.schema import DatabaseSchemaEditor
from django.db.backends.utils import CursorWrapper, truncate_name

from django_pgviews.management.operations._utils import _make_where, _schema_and_name

if TYPE_CHECKING:
    from django_pgviews.view import MaterializedView, View

logger = logging.getLogger("django_pgviews.view")


def _create_mat_view(cursor: CursorWrapper, view_name: str, query: str, params: Any, with_data: bool) -> None:
    """
    Creates a materialized view using a specific cursor, name and definition.
    """
    cursor.execute(
        "CREATE MATERIALIZED VIEW {} AS {} {};".format(view_name, query, "WITH DATA" if with_data else "WITH NO DATA"),
        params,
    )


def _drop_mat_view(cursor: CursorWrapper, view_name: str) -> None:
    """
    Drops a materialized view using a specific cursor.
    """
    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")


def _concurrent_index_name(view_name: str, concurrent_index: str) -> str:
    # replace . with _ in view_name in case the table is in a schema
    return view_name.replace(".", "_") + "_" + "_".join([s.strip() for s in concurrent_index.split(",")]) + "_index"


def _create_concurrent_index(cursor: CursorWrapper, view_name: str, concurrent_index: str) -> None:
    cursor.execute(
        f"CREATE UNIQUE INDEX {_concurrent_index_name(view_name, concurrent_index)} ON {view_name} ({concurrent_index})"
    )


class CustomSchemaEditor(DatabaseSchemaEditor):
    def _create_index_sql(self, *args, **kwargs):
        """
        Override to handle indexes in custom schemas, when the schema is explicitly set.
        """
        statement = super()._create_index_sql(*args, **kwargs)

        model = args[0]

        if "." in model._meta.db_table:  # by default the table it's quoted, but we need it non-quoted
            statement.parts["table"] = model._meta.db_table

        return statement


def _ensure_indexes(
    connection: DatabaseWrapper, cursor: CursorWrapper, view_cls: type[MaterializedView], schema_name_log: str
) -> None:
    """
    This function gets called when a materialized view is deemed not needing a re-create. That is however only a part
    of the story, since that checks just the SQL of the view itself. The second part is the indexes.
    This function gets the current indexes on the materialized view and reconciles them with the indexes that
    should be in the view, dropping extra ones and creating new ones.
    """
    view_name = view_cls._meta.db_table

    concurrent_index = view_cls._concurrent_index
    indexes = view_cls._meta.indexes
    vschema, vname = _schema_and_name(connection, view_name)

    where_fragment, params = _make_where(schemaname=vschema, tablename=vname)
    cursor.execute(f"SELECT indexname FROM pg_indexes WHERE {where_fragment}", params)

    existing_indexes = {x[0] for x in cursor.fetchall()}
    required_indexes = {x.name for x in indexes}

    if view_cls._concurrent_index is not None:
        concurrent_index_name = _concurrent_index_name(view_name, concurrent_index)
        required_indexes.add(concurrent_index_name)
    else:
        concurrent_index_name = None

    for index_name in existing_indexes - required_indexes:
        if vschema:
            full_index_name = f"{vschema}.{index_name}"
        else:
            full_index_name = index_name
        cursor.execute(f"DROP INDEX {full_index_name}")
        logger.info("pgview dropped index %s on view %s (%s)", index_name, view_name, schema_name_log)

    schema_editor: DatabaseSchemaEditor = CustomSchemaEditor(connection)

    for index_name in required_indexes - existing_indexes:
        if index_name == concurrent_index_name:
            _create_concurrent_index(cursor, view_name, concurrent_index)
            logger.info("pgview created concurrent index on view %s (%s)", view_name, schema_name_log)
        else:
            for index in indexes:
                if index.name == index_name:
                    schema_editor.add_index(view_cls, index)
                    logger.info("pgview created index %s on view %s (%s)", index.name, view_name, schema_name_log)
                    break


@transaction.atomic()
def create_materialized_view(
    connection: DatabaseWrapper, view_cls: type[View], check_sql_changed: bool = False
) -> Literal["EXISTS", "UPDATED", "CREATED"]:
    """
    Create a materialized view on a connection.

    Returns one of statuses EXISTS, UPDATED, CREATED.

    If with_data = False, then the materialized view will get created without data.

    If check_sql_changed = True, then the process will first check if there is a materialized view in the database
    already with the same SQL, if there is, it will not do anything. Otherwise the materialized view gets dropped
    and recreated.
    """
    view_name = view_cls._meta.db_table
    view_query = view_cls.get_sql()

    concurrent_index = view_cls._concurrent_index

    try:
        schema_name = connection.schema_name
        schema_name_log = f"schema {schema_name}"
    except AttributeError:
        schema_name_log = "default schema"

    vschema, vname = _schema_and_name(connection, view_name)

    cursor_wrapper = connection.cursor()
    cursor = cursor_wrapper.cursor

    where_fragment, params = _make_where(schemaname=vschema, matviewname=vname)

    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM pg_matviews WHERE {where_fragment};",
            params,
        )
        view_exists = cursor.fetchone()[0] > 0

        query = view_query.query.strip()
        if query.endswith(";"):
            query = query[:-1]

        if check_sql_changed and view_exists:
            temp_viewname = truncate_name(view_name + "_temp", length=63)
            _, temp_vname = _schema_and_name(connection, temp_viewname)

            _drop_mat_view(cursor, temp_viewname)
            _create_mat_view(cursor, temp_viewname, query, view_query.params, with_data=False)

            definitions_where, definitions_params = _make_where(schemaname=vschema, matviewname=[vname, temp_vname])
            cursor.execute(
                f"SELECT definition FROM pg_matviews WHERE {definitions_where};",
                definitions_params,
            )
            definitions = cursor.fetchall()

            _drop_mat_view(cursor, temp_viewname)

            if definitions[0] == definitions[1]:
                _ensure_indexes(connection, cursor, view_cls, schema_name_log)

                if view_cls.with_data:
                    definitions_where, definitions_params = _make_where(schemaname=vschema, matviewname=[vname])
                    cursor.execute(
                        f"SELECT ispopulated FROM pg_catalog.pg_matviews WHERE {definitions_where}",
                        definitions_params,
                    )
                    has_data = cursor.fetchone()[0]

                    if not has_data:
                        view_cls.refresh(concurrently=False)

                return "EXISTS"

        if view_exists:
            _drop_mat_view(cursor, view_name)
            logger.info("pgview dropped materialized view %s (%s)", view_name, schema_name_log)

        _create_mat_view(cursor, view_name, query, view_query.params, with_data=view_cls.with_data)
        logger.info("pgview created materialized view %s (%s)", view_name, schema_name_log)

        if concurrent_index is not None:
            _create_concurrent_index(cursor, view_name, concurrent_index)
            logger.info("pgview created concurrent index on view %s (%s)", view_name, schema_name_log)

        if view_cls._meta.indexes:
            schema_editor = CustomSchemaEditor(connection)

            for index in view_cls._meta.indexes:
                schema_editor.add_index(view_cls, index)
                logger.info("pgview created index %s on view %s (%s)", index.name, view_name, schema_name_log)

        if view_exists:
            return "UPDATED"

        return "CREATED"
    finally:
        cursor_wrapper.close()
