from django.apps import apps
from django.core.management import load_command_class
from django.core.management.commands.makemigrations import Command as MakeMigrationsCommand

from django_pgviews.db.migrations.autodetector import PGViewsAutodetector


def get_base_makemigrations_command():
    """
    Find the makemigrations command from apps loaded before django_pgviews.

    This ensures compatibility with other packages that override makemigrations
    (e.g., django-linear-migrations) by inheriting from their command instead
    of Django's base command directly.
    """
    for app_config in apps.get_app_configs():
        # Stop when we reach django_pgviews to avoid circular dependency
        if app_config.name == "django_pgviews":
            break
        try:
            # Try to load makemigrations command from this app
            return load_command_class(app_config.name, "makemigrations")
        except (ImportError, AttributeError):
            # This app doesn't have a custom makemigrations command
            continue
    # No custom command found, use Django's default
    return MakeMigrationsCommand


class Command(get_base_makemigrations_command()):
    """Django 6.0 compatible makemigrations with PGViewsAutodetector."""

    autodetector = PGViewsAutodetector
