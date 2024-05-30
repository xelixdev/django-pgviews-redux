import os

from .base import *  # noqa: F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DB_NAME", "circle_test"),
        "USER": os.environ.get("DB_USER", "postgres"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
        "HOST": "localhost",
        "PORT": "5432",
    },
    "weather_db": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DB_NAME_WEATHER", "weatherdb"),
        "USER": os.environ.get("DB_USER", "postgres"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
        "HOST": "localhost",
        "PORT": "5432",
    },
    "schema_db": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DB_NAME_WEATHER", "schemadb"),
        "USER": os.environ.get("DB_USER", "postgres"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
        "HOST": "localhost",
        "PORT": "5432",
        "OPTIONS": {"options": "-c search_path=other"},
    },
}
