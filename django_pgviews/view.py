"""Helpers to access Postgres views from the Django ORM."""

from __future__ import annotations

__all__ = [
    "View",
    "ReadOnlyView",
    "MaterializedView",
    "ReadOnlyMaterializedView",
    "ViewSQL",
]

import collections
import copy
import logging
import re
from typing import Any, NamedTuple, cast

from django.apps import apps
from django.core import exceptions
from django.db import connections, models, router
from django.db.backends.postgresql.base import DatabaseWrapper

from django_pgviews.db.fields import get_fields_by_name
from django_pgviews.exceptions import ConcurrentIndexNotDefinedError, SQLNotDefinedError
from django_pgviews.managers import ReadOnlyViewManager

FIELD_SPEC_REGEX = r"^([A-Za-z_][A-Za-z0-9_]*)\." r"([A-Za-z_][A-Za-z0-9_]*)\." r"(\*|(?:[A-Za-z_][A-Za-z0-9_]*))$"
FIELD_SPEC_RE = re.compile(FIELD_SPEC_REGEX)


class ViewSQL(NamedTuple):
    query: str
    params: list[Any] | None


logger = logging.getLogger("django_pgviews.view")


def _hasfield(model_cls: type[models.Model], field_name: str) -> bool:
    """
    Like `hasattr()`, but for model fields.

    >>> from django.contrib.auth.models import User
    >>> _hasfield(User, 'password')
    True
    >>> _hasfield(User, 'foobarbaz')
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


def realize_deferred_projections(sender: Any, *args: Any, **kwargs: Any):
    """Project any fields which were deferred pending model preparation."""
    app_label = sender._meta.app_label
    model_name = sender.__name__.lower()
    pending = _DEFERRED_PROJECTIONS.pop((app_label, model_name), {})
    for view_cls, field_names in pending.items():
        field_instances = get_fields_by_name(sender, *field_names)
        for name, field in field_instances.items():
            # Only assign the field if the view does not already have an
            # attribute or explicitly-defined field with that name.
            if hasattr(view_cls, name) or _hasfield(view_cls, name):
                continue
            copy.copy(field).contribute_to_class(view_cls, name)


models.signals.class_prepared.connect(realize_deferred_projections)


class ViewMeta(models.base.ModelBase):
    def __new__(cls, name, bases, attrs):
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
                    raise TypeError(f"Unrecognized field specifier: {field_name!r}")
                deferred_projections.append(match.groups())
            else:
                raise TypeError(f"Unrecognized field specifier: {field_name!r}")

        view_cls: type[View] = models.base.ModelBase.__new__(cls, name, bases, attrs)

        # Get dependencies
        view_cls._dependencies = dependencies
        # Materialized views can have an index allowing concurrent refresh
        view_cls._concurrent_index = concurrent_index
        for app_label, model_name, field_name in deferred_projections:
            model_spec = (app_label, model_name.lower())

            _DEFERRED_PROJECTIONS[model_spec][view_cls].append(field_name)
            _realise_projections(app_label, model_name)

        return view_cls

    def add_to_class(self, name, value):
        if name == "_base_manager":
            return
        super().add_to_class(name, value)  # type: ignore[missing-attribute]


class BaseManagerMeta:
    base_manager_name = "objects"


class View(models.Model, metaclass=ViewMeta):
    """
    Helper for exposing Postgres views as Django models.
    """

    _deferred = False
    sql: str | None = None
    _dependencies: list[str]

    @classmethod
    def get_sql(cls):
        if cls.sql is None:
            raise SQLNotDefinedError(f"View SQL not defined for {cls.__name__}")
        return ViewSQL(cls.sql, None)

    @classmethod
    def get_view_connection(cls, using: str, restricted_mode: bool = True) -> DatabaseWrapper | None:
        """
        Returns connection for "using" database.
        Operates in two modes, regular mode and restricted mode.
            In regular mode just returns the connection.
            In restricted mode, returns None if migrations are not allowed (via router) to indicate view should not be
              used on the specified database.

        Overwrite this method in subclass to customize, if needed.
        """
        if not restricted_mode or router.allow_migrate(using, cls._meta.app_label):
            return cast(DatabaseWrapper, connections[using])
        return None

    class Meta:
        abstract = True
        managed = False


def _realise_projections(app_label: str, model_name: str) -> None:
    """
    Checks whether the model has been loaded and runs realise_deferred_projections() if it has.
    """
    try:
        model_cls = apps.get_model(app_label, model_name)
    except exceptions.AppRegistryNotReady:
        return
    if model_cls is not None:
        realize_deferred_projections(model_cls)


class ReadOnlyView(View):
    """View which cannot be altered"""

    _base_manager = ReadOnlyViewManager()
    objects = ReadOnlyViewManager()

    class Meta(BaseManagerMeta):
        abstract = True
        managed = False


class MaterializedView(View):
    """
    A materialized view.
    More information:
    http://www.postgresql.org/docs/current/static/sql-creatematerializedview.html
    """

    with_data: bool = True
    _concurrent_index: str | None

    @classmethod
    def refresh(cls, concurrently: bool = False, strict: bool = False) -> None:
        conn = cls.get_view_connection(using=router.db_for_write(cls), restricted_mode=False)
        if not conn:
            logger.warning("Failed to find connection to refresh %s", cls)
            return

        if concurrently and cls._concurrent_index is None:
            if strict:
                raise ConcurrentIndexNotDefinedError(
                    f"Cannot refresh concurrently without concurrent index on {cls.__name__}"
                )
            else:
                logger.warning("Cannot refresh concurrently without concurrent index on %s", cls.__name__)

        cursor_wrapper = conn.cursor()
        cursor = cursor_wrapper.cursor
        try:
            if cls._concurrent_index is not None and concurrently:
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {cls._meta.db_table}")
            else:
                cursor.execute(f"REFRESH MATERIALIZED VIEW {cls._meta.db_table}")
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
