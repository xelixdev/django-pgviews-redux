"""Test Django PGViews."""

from contextlib import closing
from datetime import timedelta

import pytest
from django.apps import apps
from django.conf import settings
from django.contrib import auth
from django.contrib.auth.models import User
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connection
from django.db.models.signals import post_migrate
from django.db.utils import DatabaseError, OperationalError
from django.dispatch import receiver
from django.test import TestCase, override_settings
from django.utils import timezone

from django_pgviews.exceptions import ConcurrentIndexNotDefinedError
from django_pgviews.management.operations._utils import _make_where, _schema_and_name
from django_pgviews.signals import all_views_synced, view_synced

from . import models
from .models import LatestSuperusers

try:
    from psycopg.errors import UndefinedTable
except ImportError:
    from psycopg2.errors import UndefinedTable


def get_list_of_indexes(cursor, cls):
    schema, table = _schema_and_name(cursor.connection, cls._meta.db_table)
    where_fragment, params = _make_where(tablename=table, schemaname=schema)
    cursor.execute(f"SELECT indexname FROM pg_indexes WHERE {where_fragment}", params)
    return {x[0] for x in cursor.fetchall()}


class ViewTestCase(TestCase):
    """
    Run the tests to ensure the post_migrate hooks were called.
    """

    def test_views_have_been_created(self):
        """
        Look at the PG View table to ensure views were created.
        """
        with closing(connection.cursor()) as cur:
            cur.execute("""SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'viewtest_%';""")

            (count,) = cur.fetchone()
            assert count == 5

            cur.execute("""SELECT COUNT(*) FROM pg_matviews WHERE matviewname LIKE 'viewtest_%';""")

            (count,) = cur.fetchone()
            assert count == 5

            cur.execute("""SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'test_schema';""")

            (count,) = cur.fetchone()
            assert count == 1

    def test_clear_views(self):
        """
        Check the PG View table to see that the views were removed.
        """
        call_command(
            "clear_pgviews",
            *[],
        )
        with closing(connection.cursor()) as cur:
            cur.execute("""SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'viewtest_%';""")

            (count,) = cur.fetchone()
            assert count == 0

            cur.execute("""SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'test_schema';""")

            (count,) = cur.fetchone()
            assert count == 0

    def test_wildcard_projection(self):
        """
        Wildcard projections take all fields from a projected model.
        """
        foo_user = auth.models.User.objects.create(username="foo", is_superuser=True)
        foo_user.set_password("blah")
        foo_user.save()

        foo_superuser = models.Superusers.objects.get(username="foo")

        assert foo_user.id == foo_superuser.id
        assert foo_user.password == foo_superuser.password

    def test_limited_projection(self):
        """
        A limited projection only creates the projected fields.
        """
        foo_user = auth.models.User.objects.create(username="foo", is_superuser=True)
        foo_user.set_password("blah")
        foo_user.save()

        foo_simple = models.SimpleUser.objects.get(username="foo")

        assert foo_simple.username == foo_user.username
        assert foo_simple.password == foo_user.password
        assert not getattr(foo_simple, "date_joined", False)

    def test_related_delete(self):
        """
        Test views do not interfere with deleting the models
        """
        test_model = models.TestModel()
        test_model.name = "Bob"
        test_model.save()
        test_model.delete()

    def test_materialized_view(self):
        """
        Test a materialized view works correctly
        """
        assert models.MaterializedRelatedView.objects.count() == 0, "Materialized view should not have anything"

        test_model = models.TestModel()
        test_model.name = "Bob"
        test_model.save()

        assert models.MaterializedRelatedView.objects.count() == 0, "Materialized view should not have anything"

        models.MaterializedRelatedView.refresh()

        assert models.MaterializedRelatedView.objects.count() == 1, "Materialized view should have updated"

        with pytest.raises(ConcurrentIndexNotDefinedError):
            models.MaterializedRelatedView.refresh(concurrently=True, strict=True)

        models.MaterializedRelatedViewWithIndex.refresh(concurrently=True, strict=True)

        assert models.MaterializedRelatedViewWithIndex.objects.count() == 1, (
            "Materialized view should have updated concurrently"
        )

    def test_refresh_missing(self):
        with connection.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW viewtest_materializedrelatedview CASCADE;")

        with pytest.raises(UndefinedTable):
            models.MaterializedRelatedView.refresh()

    def test_materialized_view_indexes(self):
        with connection.cursor() as cursor:
            orig_indexes = get_list_of_indexes(cursor, models.MaterializedRelatedViewWithIndex)
            assert "viewtest_materializedrelatedviewwithindex_id_index" in orig_indexes
            assert len(orig_indexes) == 2

            # drop current indexes, add some random ones which will get deleted
            for index_name in orig_indexes:
                cursor.execute(f"DROP INDEX {index_name}")

            cursor.execute(
                "CREATE UNIQUE INDEX viewtest_materializedrelatedviewwithindex_concurrent_idx "
                "ON viewtest_materializedrelatedviewwithindex (id)"
            )
            cursor.execute(
                "CREATE INDEX viewtest_materializedrelatedviewwithindex_some_idx "
                "ON viewtest_materializedrelatedviewwithindex (model_id)"
            )

        call_command("sync_pgviews", materialized_views_check_sql_changed=True)

        with connection.cursor() as cursor:
            new_indexes = get_list_of_indexes(cursor, models.MaterializedRelatedViewWithIndex)

            assert new_indexes == orig_indexes

    def test_materialized_view_schema_indexes(self):
        with connection.cursor() as cursor:
            orig_indexes = get_list_of_indexes(cursor, models.CustomSchemaMaterializedRelatedViewWithIndex)

            assert len(orig_indexes) == 2
            assert "test_schema_my_custom_view_with_index_id_index" in orig_indexes

            # drop current indexes, add some random ones which will get deleted
            for index_name in orig_indexes:
                cursor.execute(f"DROP INDEX test_schema.{index_name}")

            cursor.execute(
                "CREATE UNIQUE INDEX my_custom_view_with_index_concurrent_idx "
                "ON test_schema.my_custom_view_with_index (id)"
            )
            cursor.execute(
                "CREATE INDEX my_custom_view_with_index_some_idx ON test_schema.my_custom_view_with_index (model_id)"
            )

        call_command("sync_pgviews", materialized_views_check_sql_changed=True)

        with connection.cursor() as cursor:
            new_indexes = get_list_of_indexes(cursor, models.CustomSchemaMaterializedRelatedViewWithIndex)

            assert new_indexes == orig_indexes

    def test_materialized_view_with_no_data(self):
        """
        Test a materialized view with no data works correctly
        """
        with pytest.raises(OperationalError):
            models.MaterializedRelatedViewWithNoData.objects.count()

    def test_materialized_view_with_no_data_after_refresh(self):
        models.TestModel.objects.create(name="Bob")

        models.MaterializedRelatedViewWithNoData.refresh()

        assert models.MaterializedRelatedViewWithNoData.objects.count() == 1, "Materialized view should have updated"

    def test_signals(self):
        expected = {
            models.MaterializedRelatedView: {"status": "UPDATED", "has_changed": True},
            models.Superusers: {"status": "EXISTS", "has_changed": False},
        }
        synced_views = []
        all_views_were_synced = [False]

        @receiver(view_synced)
        def on_view_synced(sender, **kwargs):
            synced_views.append(sender)
            if sender in expected:
                expected_kwargs = expected.pop(sender)
                assert (
                    expected_kwargs
                    | {"update": False, "force": False, "signal": view_synced, "using": DEFAULT_DB_ALIAS}
                    == kwargs
                )

        @receiver(all_views_synced)
        def on_all_views_synced(sender, **kwargs):
            all_views_were_synced[0] = True

        call_command("sync_pgviews", update=False)

        # All views went through syncing
        assert all_views_were_synced[0] is True
        assert not expected
        assert len(synced_views) == 12

    def test_get_sql(self):
        User.objects.create(username="old", is_superuser=True, date_joined=timezone.now() - timedelta(days=10))
        User.objects.create(username="new", is_superuser=True, date_joined=timezone.now() - timedelta(days=1))

        call_command("sync_pgviews", update=False)

        assert LatestSuperusers.objects.count() == 1

    def test_sync_pgviews_materialized_views_check_sql_changed(self):
        assert models.TestModel.objects.count() == 0, "Test started with non-empty TestModel"
        assert models.MaterializedRelatedView.objects.count() == 0, "Test started with non-empty mat view"

        models.TestModel.objects.create(name="Test")

        # test regular behaviour, the mat view got recreated
        call_command("sync_pgviews", update=False)  # uses default django setting, False
        assert models.MaterializedRelatedView.objects.count() == 1

        # the mat view did not get recreated because the model hasn't changed
        models.TestModel.objects.create(name="Test 2")
        call_command("sync_pgviews", update=False, materialized_views_check_sql_changed=True)
        assert models.MaterializedRelatedView.objects.count() == 1

        # the mat view got recreated because the mat view SQL has changed

        # let's pretend the mat view in the DB is ordered by name, while the defined on models isn't
        with connection.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW viewtest_materializedrelatedview CASCADE;")
            cursor.execute(
                """
                CREATE MATERIALIZED VIEW viewtest_materializedrelatedview as
                SELECT id AS model_id, id FROM viewtest_testmodel ORDER BY name;
                """
            )

        call_command("sync_pgviews", update=False, materialized_views_check_sql_changed=True)
        assert models.MaterializedRelatedView.objects.count() == 2

    def test_migrate_materialized_views_check_sql_changed_default(self):
        assert models.TestModel.objects.count() == 0, "Test started with non-empty TestModel"
        assert models.MaterializedRelatedView.objects.count() == 0, "Test started with non-empty mat view"

        models.TestModel.objects.create(name="Test")

        call_command("migrate")

        assert models.MaterializedRelatedView.objects.count() == 1

    def test_refresh_pgviews(self):
        models.TestModel.objects.create(name="Test")

        call_command("refresh_pgviews")

        assert models.MaterializedRelatedView.objects.count() == 1
        assert models.DependantView.objects.count() == 1
        assert models.DependantMaterializedView.objects.count() == 1
        assert models.MaterializedRelatedViewWithIndex.objects.count() == 1
        assert models.MaterializedRelatedViewWithNoData.objects.count() == 1

        models.TestModel.objects.create(name="Test 2")

        call_command("refresh_pgviews", concurrently=True)

        assert models.MaterializedRelatedView.objects.count() == 2
        assert models.DependantView.objects.count() == 2
        assert models.DependantMaterializedView.objects.count() == 2
        assert models.MaterializedRelatedViewWithIndex.objects.count() == 2
        assert models.MaterializedRelatedViewWithNoData.objects.count() == 2

        with pytest.raises(ConcurrentIndexNotDefinedError):
            call_command("refresh_pgviews", concurrently=True, strict=True)


class TestMaterializedViewsCheckSQLSettings(TestCase):
    def setUp(self):
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = True

    def test_migrate_materialized_views_check_sql_set_to_true(self):
        assert models.TestModel.objects.count() == 0
        assert models.MaterializedRelatedView.objects.count() == 0

        models.TestModel.objects.create(name="Test")
        call_command("migrate")
        assert models.MaterializedRelatedView.objects.count() == 0

        # let's pretend the mat view in the DB is ordered by name, while the defined on models isn't
        with connection.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW viewtest_materializedrelatedview CASCADE;")
            cursor.execute(
                """
                CREATE MATERIALIZED VIEW viewtest_materializedrelatedview as
                SELECT id AS model_id, id FROM viewtest_testmodel ORDER BY name;
                """
            )

        # which means that when the sync is triggered here, the mat view will get updated
        call_command("migrate")
        assert models.MaterializedRelatedView.objects.count() == 1

    def tearDown(self):
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = False


class DependantViewTestCase(TestCase):
    def test_sync_depending_views(self):
        """
        Test the sync_pgviews command for views that depend on other views.

        This test drops `viewtest_dependantview` and its dependencies
        and recreates them manually, thereby simulating an old state
        of the views in the db before changes to the view model's sql is made.
        Then we sync the views again and verify that everything was updated.
        """

        with closing(connection.cursor()) as cur:
            cur.execute("DROP VIEW viewtest_relatedview CASCADE;")

            cur.execute("""CREATE VIEW viewtest_relatedview as SELECT id AS model_id, name FROM viewtest_testmodel;""")

            cur.execute("""CREATE VIEW viewtest_dependantview as SELECT name from viewtest_relatedview;""")

            cur.execute("""SELECT name from viewtest_relatedview;""")
            cur.execute("""SELECT name from viewtest_dependantview;""")

        call_command("sync_pgviews", "--force")

        with closing(connection.cursor()) as cur:
            cur.execute("""SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'viewtest_%';""")

            (count,) = cur.fetchone()
            assert count == 5

            with pytest.raises(DatabaseError):
                cur.execute("""SELECT name from viewtest_relatedview;""")

            with pytest.raises(DatabaseError):
                cur.execute("""SELECT name from viewtest_dependantview;""")

    def test_sync_depending_materialized_views(self):
        """
        Refresh views that depend on materialized views.
        """
        with closing(connection.cursor()) as cur:
            cur.execute(
                """DROP MATERIALIZED VIEW viewtest_materializedrelatedview
                CASCADE;"""
            )

            cur.execute(
                """CREATE MATERIALIZED VIEW viewtest_materializedrelatedview as
                SELECT id AS model_id, name FROM viewtest_testmodel;"""
            )

            cur.execute(
                """CREATE MATERIALIZED VIEW viewtest_dependantmaterializedview
                as SELECT name from viewtest_materializedrelatedview;"""
            )
            cur.execute("""SELECT name from viewtest_materializedrelatedview;""")
            cur.execute("""SELECT name from viewtest_dependantmaterializedview;""")

        call_command("sync_pgviews", "--force")

        with closing(connection.cursor()) as cur:
            cur.execute("""SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'viewtest_%';""")

            (count,) = cur.fetchone()
            assert count == 5

            with pytest.raises(DatabaseError):
                cur.execute("""SELECT name from viewtest_dependantmaterializedview;""")

            with pytest.raises(DatabaseError):
                cur.execute("""SELECT name from viewtest_materializedrelatedview; """)

            with pytest.raises(DatabaseError):
                cur.execute("""SELECT name from viewtest_dependantmaterializedview;""")


class MakeWhereTestCase(TestCase):
    def test_with_schema(self):
        where_fragment, params = _make_where(schemaname="test_schema", tablename="test_tablename")
        assert where_fragment == "schemaname = %s AND tablename = %s"
        assert params == ["test_schema", "test_tablename"]

    def test_no_schema(self):
        where_fragment, params = _make_where(schemaname=None, tablename="test_tablename")
        assert where_fragment == "tablename = %s"
        assert params == ["test_tablename"]

    def test_with_schema_list(self):
        where_fragment, params = _make_where(schemaname="test_schema", tablename=["test_tablename1", "test_tablename2"])
        assert where_fragment == "schemaname = %s AND tablename IN (%s, %s)"
        assert params == ["test_schema", "test_tablename1", "test_tablename2"]

    def test_no_schema_list(self):
        where_fragment, params = _make_where(schemaname=None, tablename=["test_tablename1", "test_tablename2"])
        assert where_fragment == "tablename IN (%s, %s)"
        assert params == ["test_tablename1", "test_tablename2"]


class TestMaterializedViewSyncDisabledSettings(TestCase):
    def setUp(self):
        """
        NOTE: By default, Django runs and registers signals with default values during
        test execution. To address this, we store the original receivers and settings,
        then restore them in tearDown to avoid affecting other tests.
        """

        # Store original receivers and settings
        self._original_receivers = list(post_migrate.receivers)
        self._original_config = apps.get_app_config("django_pgviews").counter

        # Clear existing signal receivers
        post_migrate.receivers.clear()

        # Get the app config and reset counter
        config = apps.get_app_config("django_pgviews")
        config.counter = 0

        # Reload app config with new settings
        with override_settings(MATERIALIZED_VIEWS_DISABLE_SYNC_ON_MIGRATE=True):
            config.ready()

        # Drop the view if it exists
        with connection.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW IF EXISTS viewtest_materializedrelatedview CASCADE;")

    def tearDown(self):
        """Restore original signal receivers and app config state"""

        post_migrate.receivers.clear()
        post_migrate.receivers.extend(self._original_receivers)
        apps.get_app_config("django_pgviews").counter = self._original_config

    def test_migrate_materialized_views_sync_disabled(self):
        assert models.TestModel.objects.count() == 0

        models.TestModel.objects.create(name="Test")

        call_command("migrate")  # migrate is not running sync_pgviews
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'viewtest_materializedrelatedview');"
            )
            exists = cursor.fetchone()[0]
            assert not exists, "Materialized view viewtest_materializedrelatedview should not exist."

        call_command("sync_pgviews")  # explicitly run sync_pgviews
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'viewtest_materializedrelatedview');"
            )
            exists = cursor.fetchone()[0]
            assert exists, "Materialized view viewtest_materializedrelatedview should exist."
