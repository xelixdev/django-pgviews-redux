from django.core.management.commands.migrate import Command as MigrateCommand

from django_pgviews.db.migrations.autodetector import PGViewsAutodetector


class Command(MigrateCommand):
    autodetector = PGViewsAutodetector
