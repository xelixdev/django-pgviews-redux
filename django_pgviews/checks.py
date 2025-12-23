from typing import Any

from django.core.checks import CheckMessage, Tags, register
from django.core.checks import Warning as CheckWarning

from django_pgviews.db.migrations.autodetector import PGViewsAutodetector


@register(Tags.database)  # type: ignore[not-callable]
def validate_has_pgviews_autodetector(**kwargs: Any) -> list[CheckMessage]:
    from django.core.management import get_commands, load_command_class

    commands = get_commands()

    make_migrations = load_command_class(commands["makemigrations"], "makemigrations")
    migrate = load_command_class(commands["migrate"], "migrate")

    if not issubclass(
        migrate.autodetector,  # type: ignore[missing-attribute]
        PGViewsAutodetector,
    ) or not issubclass(
        make_migrations.autodetector,  # type: ignore[missing-attribute]
        PGViewsAutodetector,
    ):
        return [
            CheckWarning(
                (
                    "If you don't use PGViewsAutodetector on your migrate and makemigrations commands, "
                    "django_pgviews will not detect and delete that views have been removed. "
                    "You are seeing this because you or some other dependency has overwritten the commands "
                    "from django_pgviews. "
                ),
                hint=(
                    f"The makemigrations.Command.autodetector is {make_migrations.autodetector.__name__}, "  # type: ignore[missing-attribute]
                    f"the migrate.Command.autodetector is {migrate.autodetector.__name__}."  # type: ignore[missing-attribute]
                ),
                id="django_pgviews.W001",
            )
        ]

    return []
