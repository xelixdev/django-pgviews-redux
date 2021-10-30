from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from django_pgviews.models import ViewRefresher


class Command(BaseCommand):
    help = """Refresh materialized Postgres views for all installed apps."""

    def add_arguments(self, parser):
        parser.add_argument(
            "-C",
            "--concurrently",
            action="store_true",
            dest="concurrently",
            help="Refresh concurrently if the materialized view supports it",
        )
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help='Nominates a database to synchronize. Defaults to the "default" database.',
        )

    def handle(self, concurrently, database, **options):
        ViewRefresher().run(concurrently, using=database)
