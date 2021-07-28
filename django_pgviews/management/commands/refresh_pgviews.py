from django.core.management.base import BaseCommand

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

    def handle(self, concurrently, **options):
        ViewRefresher().run(concurrently)
