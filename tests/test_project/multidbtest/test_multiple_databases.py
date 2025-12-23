import datetime as dt

from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connections
from django.dispatch import receiver
from django.test import TestCase

from django_pgviews.signals import view_synced
from tests.test_project.multidbtest.models import MonthlyObservation, Observation
from tests.test_project.viewtest.models import RelatedView


class WeatherPinnedViewConnectionTest(TestCase):
    """Weather views should only return weather_db when pinned."""

    def test_weather_view_using_weather_db(self):
        self.assertEqual(MonthlyObservation.get_view_connection(using="weather_db"), connections["weather_db"])

    def test_weather_view_using_default_db(self):
        self.assertIsNone(MonthlyObservation.get_view_connection(using=DEFAULT_DB_ALIAS))

    def test_other_app_view_using_weather_db(self):
        self.assertIsNone(RelatedView.get_view_connection(using="weather_db"))

    def test_other_app_view_using_default_db(self):
        self.assertEqual(RelatedView.get_view_connection(using=DEFAULT_DB_ALIAS), connections["default"])


class WeatherPinnedRefreshViewTest(TestCase):
    """View.refresh() should automatically select the appropriate database."""

    databases = {DEFAULT_DB_ALIAS, "weather_db"}

    def test_pre_refresh(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        Observation.objects.create(date=dt.date(2022, 1, 3), temperature=20)
        self.assertEqual(MonthlyObservation.objects.count(), 0)

    def test_refresh(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        Observation.objects.create(date=dt.date(2022, 1, 3), temperature=20)
        MonthlyObservation.refresh()
        self.assertEqual(MonthlyObservation.objects.count(), 1)


class WeatherPinnedMigrateTest(TestCase):
    """Ensure views are only sync'd against the correct database on migrate."""

    databases = {DEFAULT_DB_ALIAS, "weather_db"}

    def test_default(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("migrate", database=DEFAULT_DB_ALIAS)
        self.assertNotIn(MonthlyObservation, synced_views)
        self.assertIn(RelatedView, synced_views)

    def test_weather_db(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("migrate", database="weather_db")
        self.assertIn(MonthlyObservation, synced_views)
        self.assertNotIn(RelatedView, synced_views)


class WeatherPinnedSyncPGViewsTest(TestCase):
    """Ensure views are only sync'd against the correct database with sync_pgviews."""

    databases = {DEFAULT_DB_ALIAS, "weather_db"}

    def test_default(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("sync_pgviews", database=DEFAULT_DB_ALIAS)
        self.assertNotIn(MonthlyObservation, synced_views)
        self.assertIn(RelatedView, synced_views)

    def test_weather_db(self):
        synced_views = []

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)

        call_command("sync_pgviews", database="weather_db")
        self.assertIn(MonthlyObservation, synced_views)
        self.assertNotIn(RelatedView, synced_views)


class WeatherPinnedRefreshPGViewsTest(TestCase):
    """Ensure views are only refreshed on each database using refresh_pgviews"""

    databases = {DEFAULT_DB_ALIAS, "weather_db"}

    def test_default(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        call_command("refresh_pgviews", database=DEFAULT_DB_ALIAS)
        self.assertEqual(MonthlyObservation.objects.count(), 0)

    def test_weather_db(self):
        Observation.objects.create(date=dt.date(2022, 1, 1), temperature=10)
        call_command("refresh_pgviews", database="weather_db")
        self.assertEqual(MonthlyObservation.objects.count(), 1)
