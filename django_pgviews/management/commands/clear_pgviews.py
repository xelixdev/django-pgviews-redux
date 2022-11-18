import logging

from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from django_pgviews.view import clear_view, View, MaterializedView

logger = logging.getLogger("django_pgviews.sync_pgviews")


class Command(BaseCommand):
    help = """Clear Postgres views. Use this before running a migration"""

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help='Nominates a database to synchronize. Defaults to the "default" database.',
        )

    def handle(self, database, **options):
        for view_cls in apps.get_models():
            if not (isinstance(view_cls, type) and issubclass(view_cls, View) and hasattr(view_cls, "sql")):
                continue
            python_name = "{}.{}".format(view_cls._meta.app_label, view_cls.__name__)
            connection = view_cls.get_view_connection(using=database, restricted_mode=True)
            if not connection:
                continue
            status = clear_view(
                connection, view_cls._meta.db_table, materialized=isinstance(view_cls(), MaterializedView)
            )
            if status == "DROPPED":
                msg = "dropped"
            else:
                msg = "not dropped"
            logger.info("%s (%s): %s", python_name, view_cls._meta.db_table, msg)
