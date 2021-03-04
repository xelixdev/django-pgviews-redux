import logging

from django.conf import settings
from django.core.management.base import BaseCommand

from django_pgviews.models import ViewSyncer

log = logging.getLogger("django_pgviews.sync_pgviews")


class Command(BaseCommand):
    help = """Create/update Postgres views for all installed apps."""

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-update",
            action="store_false",
            dest="update",
            default=True,
            help="""Don't update existing views, only create new ones.""",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            dest="force",
            default=False,
            help="Force replacement of pre-existing views where breaking changes have been made to the schema.",
        )
        parser.add_argument(
            "--enable-materialized-views-check-sql-changed",
            action="store_true",
            dest="materialized_views_check_sql_changed",
            default=None,
            help=(
                "Before recreating materialized view, check the SQL has changed compared to the currently active "
                "materialized view in the database, if there is one, and only re-create the materialized view "
                "if the SQL is different. By default uses django setting MATERIALIZED_VIEWS_CHECK_SQL_CHANGED."
            ),
        )
        parser.add_argument(
            "--disable-materialized-views-check-sql-changed",
            action="store_false",
            dest="materialized_views_check_sql_changed",
            default=None,
            help=(
                "Before recreating materialized view, check the SQL has changed compared to the currently active "
                "materialized view in the database, if there is one, and only re-create the materialized view "
                "if the SQL is different. By default uses django setting MATERIALIZED_VIEWS_CHECK_SQL_CHANGED."
            ),
        )

    def handle(self, force, update, materialized_views_check_sql_changed, **options):
        vs = ViewSyncer()

        if materialized_views_check_sql_changed is None:
            materialized_views_check_sql_changed = getattr(settings, "MATERIALIZED_VIEWS_CHECK_SQL_CHANGED", False)

        vs.run(force, update, materialized_views_check_sql_changed=materialized_views_check_sql_changed)
