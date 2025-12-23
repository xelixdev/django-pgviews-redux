from django.core.management.commands.makemigrations import Command as MakeMigrationsCommand

from django_pgviews.db.migrations.autodetector import PGViewsAutodetector


class Command(MakeMigrationsCommand):
    autodetector = PGViewsAutodetector
