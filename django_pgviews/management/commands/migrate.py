from django.apps import apps
from django.core.management import load_command_class
from django.core.management.commands.migrate import Command as MigrateCommand

from django_pgviews.db.migrations.autodetector import PGViewsAutodetector


def get_base_migrate_command():
    """
    Find the migrate command from apps loaded before django_pgviews.

    This ensures compatibility with other packages that override migrate
    by inheriting from their command instead of Django's base command directly.
    """
    for app_config in apps.get_app_configs():
        # Stop when we reach django_pgviews to avoid circular dependency
        if app_config.name == "django_pgviews":
            break
        try:
            # Try to load migrate command from this app
            return load_command_class(app_config.name, "migrate")
        except (ImportError, AttributeError):
            # This app doesn't have a custom migrate command
            continue
    # No custom command found, use Django's default
    return MigrateCommand


class Command(get_base_migrate_command()):
    """Django 6.0 compatible migrate with PGViewsAutodetector."""

    autodetector = PGViewsAutodetector
