import datetime as dt

import pytest
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connections
from django.dispatch import receiver

from django_pgviews.signals import view_synced
from tests.test_project.multidbtest.models import MonthlyObservation, Observation
from tests.test_project.viewtest.models import RelatedView


@pytest.mark.django_db
class TestWeatherPinnedViewConnection:
    """Weather views should only return weather_db when pinned."""

    def test_weather_view_using_weather_db(self):
        assert MonthlyObservation.get_view_connection(using="weather_db") == connections["weather_db"]

    def test_weather_view_using_default_db(self):
        assert MonthlyObservation.get_view_connection(using=DEFAULT_DB_ALIAS) is None

    def test_other_app_view_using_weather_db(self):
        assert RelatedView.get_view_connection(using="weather_db") is None

    def test_other_app_view_using_default_db(self):
        assert RelatedView.get_view_connection(using=DEFAULT_DB_ALIAS) == connections["default"]


@pytest.mark.django_db(databases=(DEFAULT_DB_ALIAS, "weather_db"))
class TestWeatherPinnedRefreshView:
    """View.refresh() should automatically select the appropriate database."""

    databases = {DEFAULT_DB_ALIAS, "weather_db"}

    def test_pre_refresh(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        Observation.objects.create(date=dt.date(2022, 1, 3), temperature=20)
        assert MonthlyObservation.objects.count() == 0

    def test_refresh(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        Observation.objects.create(date=dt.date(2022, 1, 3), temperature=20)
        MonthlyObservation.refresh()
        assert MonthlyObservation.objects.count() == 1


@pytest.mark.django_db(databases=(DEFAULT_DB_ALIAS, "weather_db"))
class TestWeatherPinnedMigrate:
    """Ensure views are only sync'd against the correct database on migrate."""

    def test_default(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("migrate", database=DEFAULT_DB_ALIAS)
        assert MonthlyObservation not in synced_views
        assert RelatedView in synced_views

    def test_weather_db(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("migrate", database="weather_db")
        assert MonthlyObservation in synced_views
        assert RelatedView not in synced_views


@pytest.mark.django_db(databases=(DEFAULT_DB_ALIAS, "weather_db"))
class TestWeatherPinnedSyncPGViews:
    """Ensure views are only sync'd against the correct database with sync_pgviews."""

    def test_default(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("sync_pgviews", database=DEFAULT_DB_ALIAS)
        assert MonthlyObservation not in synced_views
        assert RelatedView in synced_views

    def test_weather_db(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("sync_pgviews", database="weather_db")
        assert MonthlyObservation in synced_views
        assert RelatedView not in synced_views


@pytest.mark.django_db(databases=(DEFAULT_DB_ALIAS, "weather_db"))
class TestWeatherPinnedRefreshPGViews:
    """Ensure views are only refreshed on each database using refresh_pgviews"""

    def test_default(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        call_command("refresh_pgviews", database=DEFAULT_DB_ALIAS)
        assert MonthlyObservation.objects.count() == 0

    def test_weather_db(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        call_command("refresh_pgviews", database="weather_db")
        assert MonthlyObservation.objects.count() == 1
