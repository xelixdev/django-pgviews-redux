from argparse import ArgumentParser
from typing import Any

from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from django_pgviews.models import ViewRefresher


class Command(BaseCommand):
    help = """Refresh materialized Postgres views for all installed apps."""

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "-C",
            "--concurrently",
            action="store_true",
            dest="concurrently",
            help="Refresh concurrently if the materialized view supports it.",
        )
        parser.add_argument(
            "--strict",
            action="store_true",
            dest="strict",
            help="Raise error if concurrently refreshing materialized without a concurrent index.",
        )
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help='Nominates a database to synchronize. Defaults to the "default" database.',
        )

    def handle(self, concurrently: bool, strict: bool, database: str, **options: Any) -> None:
        ViewRefresher().run(concurrently=concurrently, using=database, strict=strict)
