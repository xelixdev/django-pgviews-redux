"""Helpers to access Postgres views from the Django ORM.
"""
import collections
import copy
import logging
import re

import django
import psycopg2
from django.apps import apps
from django.core import exceptions
from django.db import connections, router, transaction
from django.db import models
from django.db.backends.postgresql.schema import DatabaseSchemaEditor
from django.db.models.query import QuerySet

from django_pgviews.db import get_fields_by_name

FIELD_SPEC_REGEX = r"^([A-Za-z_][A-Za-z0-9_]*)\." r"([A-Za-z_][A-Za-z0-9_]*)\." r"(\*|(?:[A-Za-z_][A-Za-z0-9_]*))$"
FIELD_SPEC_RE = re.compile(FIELD_SPEC_REGEX)

ViewSQL = collections.namedtuple("ViewSQL", "query,params")

logger = logging.getLogger("django_pgviews.view")


def hasfield(model_cls, field_name):
    """
    Like `hasattr()`, but for model fields.

    >>> from django.contrib.auth.models import User
    >>> hasfield(User, 'password')
    True
    >>> hasfield(User, 'foobarbaz')
    False
    """
    try:
        model_cls._meta.get_field(field_name)
        return True
    except exceptions.FieldDoesNotExist:
        return False


# Projections of models fields onto views which have been deferred due to
# model import and loading dependencies.
# Format: (app_label, model_name): {view_cls: [field_name, ...]}
_DEFERRED_PROJECTIONS = collections.defaultdict(lambda: collections.defaultdict(list))


def realize_deferred_projections(sender, *args, **kwargs):
    """Project any fields which were deferred pending model preparation."""
    app_label = sender._meta.app_label
    model_name = sender.__name__.lower()
    pending = _DEFERRED_PROJECTIONS.pop((app_label, model_name), {})
    for view_cls, field_names in pending.items():
        field_instances = get_fields_by_name(sender, *field_names)
        for name, field in field_instances.items():
            # Only assign the field if the view does not already have an
            # attribute or explicitly-defined field with that name.
            if hasattr(view_cls, name) or hasfield(view_cls, name):
                continue
            copy.copy(field).contribute_to_class(view_cls, name)


models.signals.class_prepared.connect(realize_deferred_projections)


def _schema_and_name(connection, view_name):
    if "." in view_name:
        return view_name.split(".", 1)
    else:
        try:
            schema_name = connection.schema_name
        except AttributeError:
            schema_name = "public"

        return schema_name, view_name


def _create_mat_view(cursor, view_name, query, params, with_data):
    """
    Creates a materialized view using a specific cursor, name and definition.
    """
    cursor.execute(
        "CREATE MATERIALIZED VIEW {0} AS {1} {2};".format(
            view_name, query, "WITH DATA" if with_data else "WITH NO DATA"
        ),
        params,
    )


def _drop_mat_view(cursor, view_name):
    """
    Drops a materialized view using a specific cursor.
    """
    cursor.execute("DROP MATERIALIZED VIEW IF EXISTS {0} CASCADE;".format(view_name))


def _concurrent_index_name(view_name, concurrent_index):
    # replace . with _ in view_name in case the table is in a schema
    return view_name.replace(".", "_") + "_" + "_".join([s.strip() for s in concurrent_index.split(",")]) + "_index"


def _create_concurrent_index(cursor, view_name, concurrent_index):
    cursor.execute(
        "CREATE UNIQUE INDEX {index_name} ON {view_name} ({concurrent_index})".format(
            view_name=view_name,
            index_name=_concurrent_index_name(view_name, concurrent_index),
            concurrent_index=concurrent_index,
        )
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


def _ensure_indexes(connection, cursor, view_cls, schema_name_log):
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

    cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = %s AND schemaname = %s", [vname, vschema])

    existing_indexes = set(x[0] for x in cursor.fetchall())
    required_indexes = set(x.name for x in indexes)

    if view_cls._concurrent_index is not None:
        concurrent_index_name = _concurrent_index_name(view_name, concurrent_index)
        required_indexes.add(concurrent_index_name)
    else:
        concurrent_index_name = None

    for index_name in existing_indexes - required_indexes:
        cursor.execute(f"DROP INDEX {vschema}.{index_name}")
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
def create_materialized_view(connection, view_cls, check_sql_changed=False):
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

    try:
        cursor.execute(
            "SELECT COUNT(*) FROM pg_matviews WHERE schemaname = %s and matviewname = %s;",
            [vschema, vname],
        )
        view_exists = cursor.fetchone()[0] > 0

        query = view_query.query.strip()
        if query.endswith(";"):
            query = query[:-1]

        if check_sql_changed and view_exists:
            temp_viewname = view_name + "_temp"
            _, temp_vname = _schema_and_name(connection, temp_viewname)

            _drop_mat_view(cursor, temp_viewname)
            _create_mat_view(cursor, temp_viewname, query, view_query.params, with_data=False)

            cursor.execute(
                "SELECT definition FROM pg_matviews WHERE schemaname = %s and matviewname IN (%s, %s);",
                [vschema, vname, temp_vname],
            )
            definitions = cursor.fetchall()

            _drop_mat_view(cursor, temp_viewname)

            if definitions[0] == definitions[1]:
                _ensure_indexes(connection, cursor, view_cls, schema_name_log)
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


@transaction.atomic()
def create_view(connection, view_name, view_query: ViewSQL, update=True, force=False):
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
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = %s and table_name = %s;",
            [vschema, vname],
        )
        view_exists = cursor.fetchone()[0] > 0
        if view_exists and not update:
            return "EXISTS"
        elif view_exists:
            # Detect schema conflict by copying the original view, attempting to
            # update this copy, and detecting errors.
            cursor.execute("CREATE TEMPORARY VIEW check_conflict AS SELECT * FROM {0};".format(view_name))
            try:
                with transaction.atomic():
                    cursor.execute(
                        "CREATE OR REPLACE TEMPORARY VIEW check_conflict AS {0};".format(view_query.query),
                        view_query.params,
                    )
            except psycopg2.ProgrammingError:
                force_required = True
            finally:
                cursor.execute("DROP VIEW IF EXISTS check_conflict;")

        if not force_required:
            cursor.execute("CREATE OR REPLACE VIEW {0} AS {1};".format(view_name, view_query.query), view_query.params)
            ret = view_exists and "UPDATED" or "CREATED"
        elif force:
            cursor.execute("DROP VIEW IF EXISTS {0} CASCADE;".format(view_name))
            cursor.execute("CREATE VIEW {0} AS {1};".format(view_name, view_query.query), view_query.params)
            ret = "FORCED"
        else:
            ret = "FORCE_REQUIRED"

        return ret
    finally:
        cursor_wrapper.close()


def clear_view(connection, view_name, materialized=False):
    """
    Remove a named view on connection.
    """
    cursor_wrapper = connection.cursor()
    cursor = cursor_wrapper.cursor
    try:
        if materialized:
            cursor.execute("DROP MATERIALIZED VIEW IF EXISTS {0} CASCADE".format(view_name))
        else:
            cursor.execute("DROP VIEW IF EXISTS {0} CASCADE".format(view_name))
    finally:
        cursor_wrapper.close()
    return "DROPPED".format(view=view_name)


class ViewMeta(models.base.ModelBase):
    def __new__(metacls, name, bases, attrs):
        """
        Deal with all of the meta attributes, removing any Django does not want
        """
        # Get attributes before Django
        dependencies = attrs.pop("dependencies", [])
        projection = attrs.pop("projection", [])
        concurrent_index = attrs.pop("concurrent_index", None)

        # Get projection
        deferred_projections = []
        for field_name in projection:
            if isinstance(field_name, models.Field):
                attrs[field_name.name] = copy.copy(field_name)
            elif isinstance(field_name, str):
                match = FIELD_SPEC_RE.match(field_name)
                if not match:
                    raise TypeError("Unrecognized field specifier: %r" % field_name)
                deferred_projections.append(match.groups())
            else:
                raise TypeError("Unrecognized field specifier: %r" % field_name)

        view_cls = models.base.ModelBase.__new__(metacls, name, bases, attrs)

        # Get dependencies
        setattr(view_cls, "_dependencies", dependencies)
        # Materialized views can have an index allowing concurrent refresh
        setattr(view_cls, "_concurrent_index", concurrent_index)
        for app_label, model_name, field_name in deferred_projections:
            model_spec = (app_label, model_name.lower())

            _DEFERRED_PROJECTIONS[model_spec][view_cls].append(field_name)
            _realise_projections(app_label, model_name)

        return view_cls

    def add_to_class(self, name, value):
        if django.VERSION >= (1, 10) and name == "_base_manager":
            return
        super(ViewMeta, self).add_to_class(name, value)


if django.VERSION >= (1, 10):

    class BaseManagerMeta:
        base_manager_name = "objects"

else:
    BaseManagerMeta = object


class View(models.Model, metaclass=ViewMeta):
    """
    Helper for exposing Postgres views as Django models.
    """

    _deferred = False
    sql = None

    @classmethod
    def get_sql(cls):
        return ViewSQL(cls.sql, None)

    @classmethod
    def get_view_connection(cls, using, restricted_mode: bool = True):
        """
        Returns connection for "using" database.
        Operates in two modes, regular mode and restricted mode.
            In regular mode just returns the connection.
            In restricted mode, returns None if migrations are not allowed (via router) to indicate view should not be
              used on the specified database.

        Overwrite this method in subclass to customize, if needed.
        """
        if not restricted_mode or router.allow_migrate(using, cls._meta.app_label):
            return connections[using]

    class Meta:
        abstract = True
        managed = False


def _realise_projections(app_label, model_name):
    """
    Checks whether the model has been loaded and runs
    realise_deferred_projections() if it has.
    """
    try:
        model_cls = apps.get_model(app_label, model_name)
    except exceptions.AppRegistryNotReady:
        return
    if model_cls is not None:
        realize_deferred_projections(model_cls)


class ReadOnlyViewQuerySet(QuerySet):
    def _raw_delete(self, *args, **kwargs):
        return 0

    def delete(self):
        raise NotImplementedError("Not allowed")

    def update(self, **kwargs):
        raise NotImplementedError("Not allowed")

    def _update(self, values):
        raise NotImplementedError("Not allowed")

    def create(self, **kwargs):
        raise NotImplementedError("Not allowed")

    def update_or_create(self, defaults=None, **kwargs):
        raise NotImplementedError("Not allowed")

    def bulk_create(self, objs, batch_size=None):
        raise NotImplementedError("Not allowed")


class ReadOnlyViewManager(models.Manager):
    def get_queryset(self):
        return ReadOnlyViewQuerySet(self.model, using=self._db)


class ReadOnlyView(View):
    """View which cannot be altered"""

    _base_manager = ReadOnlyViewManager()
    objects = ReadOnlyViewManager()

    class Meta(BaseManagerMeta):
        abstract = True
        managed = False


class MaterializedView(View):
    """A materialized view.
    More information:
    http://www.postgresql.org/docs/current/static/sql-creatematerializedview.html
    """

    with_data = True

    @classmethod
    def refresh(self, concurrently=False):
        conn = self.get_view_connection(using=router.db_for_write(self), restricted_mode=False)
        if not conn:
            logger.warning("Failed to find connection to refresh %s", self)
            return
        cursor_wrapper = conn.cursor()
        cursor = cursor_wrapper.cursor
        try:
            if self._concurrent_index is not None and concurrently:
                cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY {0}".format(self._meta.db_table))
            else:
                cursor.execute("REFRESH MATERIALIZED VIEW {0}".format(self._meta.db_table))
        finally:
            cursor_wrapper.close()

    class Meta:
        abstract = True
        managed = False


class ReadOnlyMaterializedView(MaterializedView):
    """Read-only version of the materialized view"""

    _base_manager = ReadOnlyViewManager()
    objects = ReadOnlyViewManager()

    class Meta(BaseManagerMeta):
        abstract = True
        managed = False
