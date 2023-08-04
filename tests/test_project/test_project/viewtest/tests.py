"""Test Django PGViews.
"""
from contextlib import closing
from datetime import timedelta

from django.conf import settings
from django.contrib import auth
from django.contrib.auth.models import User
from django.core.management import call_command
from django.db import connection, DEFAULT_DB_ALIAS
from django.db.utils import OperationalError
from django.dispatch import receiver
from django.test import TestCase
from django.utils import timezone

from django_pgviews.signals import view_synced, all_views_synced
from django_pgviews.view import _schema_and_name
from . import models
from .models import LatestSuperusers

try:
    from psycopg.errors import UndefinedTable
except ImportError:
    from psycopg2.errors import UndefinedTable


def get_list_of_indexes(cursor, cls):
    schema, table = _schema_and_name(cursor.connection, cls._meta.db_table)

    cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = %s AND schemaname = %s", [table, schema])
    return set(x[0] for x in cursor.fetchall())


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
            self.assertEqual(count, 5)

            cur.execute("""SELECT COUNT(*) FROM pg_matviews WHERE matviewname LIKE 'viewtest_%';""")

            (count,) = cur.fetchone()
            self.assertEqual(count, 5)

            cur.execute("""SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'test_schema';""")

            (count,) = cur.fetchone()
            self.assertEqual(count, 1)

    def test_clear_views(self):
        """
        Check the PG View table to see that the views were removed.
        """
        call_command("clear_pgviews", *[], **{})
        with closing(connection.cursor()) as cur:
            cur.execute("""SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'viewtest_%';""")

            (count,) = cur.fetchone()
            self.assertEqual(count, 0)

            cur.execute("""SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'test_schema';""")

            (count,) = cur.fetchone()
            self.assertEqual(count, 0)

    def test_wildcard_projection(self):
        """
        Wildcard projections take all fields from a projected model.
        """
        foo_user = auth.models.User.objects.create(username="foo", is_superuser=True)
        foo_user.set_password("blah")
        foo_user.save()

        foo_superuser = models.Superusers.objects.get(username="foo")

        self.assertEqual(foo_user.id, foo_superuser.id)
        self.assertEqual(foo_user.password, foo_superuser.password)

    def test_limited_projection(self):
        """
        A limited projection only creates the projected fields.
        """
        foo_user = auth.models.User.objects.create(username="foo", is_superuser=True)
        foo_user.set_password("blah")
        foo_user.save()

        foo_simple = models.SimpleUser.objects.get(username="foo")

        self.assertEqual(foo_simple.username, foo_user.username)
        self.assertEqual(foo_simple.password, foo_user.password)
        self.assertFalse(getattr(foo_simple, "date_joined", False))

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
        self.assertEqual(
            models.MaterializedRelatedView.objects.count(), 0, "Materialized view should not have anything"
        )

        test_model = models.TestModel()
        test_model.name = "Bob"
        test_model.save()

        self.assertEqual(
            models.MaterializedRelatedView.objects.count(), 0, "Materialized view should not have anything"
        )

        models.MaterializedRelatedView.refresh()

        self.assertEqual(models.MaterializedRelatedView.objects.count(), 1, "Materialized view should have updated")

        models.MaterializedRelatedViewWithIndex.refresh(concurrently=True)

        self.assertEqual(
            models.MaterializedRelatedViewWithIndex.objects.count(),
            1,
            "Materialized view should have updated concurrently",
        )

    def test_refresh_missing(self):
        with connection.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW viewtest_materializedrelatedview CASCADE;")

        with self.assertRaises(UndefinedTable):
            models.MaterializedRelatedView.refresh()

    def test_materialized_view_indexes(self):
        with connection.cursor() as cursor:
            orig_indexes = get_list_of_indexes(cursor, models.MaterializedRelatedViewWithIndex)

            self.assertIn("viewtest_materializedrelatedviewwithindex_id_index", orig_indexes)
            self.assertEqual(len(orig_indexes), 2)

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

            self.assertEqual(new_indexes, orig_indexes)

    def test_materialized_view_schema_indexes(self):
        with connection.cursor() as cursor:
            orig_indexes = get_list_of_indexes(cursor, models.CustomSchemaMaterializedRelatedViewWithIndex)

            self.assertEqual(len(orig_indexes), 2)
            self.assertIn("test_schema_my_custom_view_with_index_id_index", orig_indexes)

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

            self.assertEqual(new_indexes, orig_indexes)

    def test_materialized_view_with_no_data(self):
        """
        Test a materialized view with no data works correctly
        """
        with self.assertRaises(OperationalError):
            models.MaterializedRelatedViewWithNoData.objects.count()

    def test_materialized_view_with_no_data_after_refresh(self):
        models.TestModel.objects.create(name="Bob")

        models.MaterializedRelatedViewWithNoData.refresh()

        self.assertEqual(
            models.MaterializedRelatedViewWithNoData.objects.count(), 1, "Materialized view should have updated"
        )

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
                self.assertEqual(
                    dict(expected_kwargs, update=False, force=False, signal=view_synced, using=DEFAULT_DB_ALIAS), kwargs
                )

        @receiver(all_views_synced)
        def on_all_views_synced(sender, **kwargs):
            all_views_were_synced[0] = True

        call_command("sync_pgviews", update=False)

        # All views went through syncing
        self.assertEqual(len(synced_views), 13)
        self.assertEqual(all_views_were_synced[0], True)
        self.assertFalse(expected)

    def test_get_sql(self):
        User.objects.create(username="old", is_superuser=True, date_joined=timezone.now() - timedelta(days=10))
        User.objects.create(username="new", is_superuser=True, date_joined=timezone.now() - timedelta(days=1))

        call_command("sync_pgviews", update=False)

        self.assertEqual(LatestSuperusers.objects.count(), 1)

    def test_sync_pgviews_materialized_views_check_sql_changed(self):
        self.assertEqual(models.TestModel.objects.count(), 0, "Test started with non-empty TestModel")
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 0, "Test started with non-empty mat view")

        models.TestModel.objects.create(name="Test")

        # test regular behaviour, the mat view got recreated
        call_command("sync_pgviews", update=False)  # uses default django setting, False
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 1)

        # the mat view did not get recreated because the model hasn't changed
        models.TestModel.objects.create(name="Test 2")
        call_command("sync_pgviews", update=False, materialized_views_check_sql_changed=True)
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 1)

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
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 2)

    def test_migrate_materialized_views_check_sql_changed_default(self):
        self.assertEqual(models.TestModel.objects.count(), 0, "Test started with non-empty TestModel")
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 0, "Test started with non-empty mat view")

        models.TestModel.objects.create(name="Test")

        call_command("migrate")

        self.assertEqual(models.MaterializedRelatedView.objects.count(), 1)

    def test_refresh_pgviews(self):
        models.TestModel.objects.create(name="Test")

        call_command("refresh_pgviews")

        self.assertEqual(models.MaterializedRelatedView.objects.count(), 1)
        self.assertEqual(models.DependantView.objects.count(), 1)
        self.assertEqual(models.DependantMaterializedView.objects.count(), 1)
        self.assertEqual(models.MaterializedRelatedViewWithIndex.objects.count(), 1)
        self.assertEqual(models.MaterializedRelatedViewWithNoData.objects.count(), 1)

        models.TestModel.objects.create(name="Test 2")

        call_command("refresh_pgviews", concurrently=True)

        self.assertEqual(models.MaterializedRelatedView.objects.count(), 2)
        self.assertEqual(models.DependantView.objects.count(), 2)
        self.assertEqual(models.DependantMaterializedView.objects.count(), 2)
        self.assertEqual(models.MaterializedRelatedViewWithIndex.objects.count(), 2)
        self.assertEqual(models.MaterializedRelatedViewWithNoData.objects.count(), 2)


class TestMaterializedViewsCheckSQLSettings(TestCase):
    def setUp(self):
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = True

    def test_migrate_materialized_views_check_sql_set_to_true(self):
        self.assertEqual(models.TestModel.objects.count(), 0)
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 0)

        models.TestModel.objects.create(name="Test")
        call_command("migrate")
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 0)

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
        self.assertEqual(models.MaterializedRelatedView.objects.count(), 1)

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
            self.assertEqual(count, 5)

            with self.assertRaises(Exception):
                cur.execute("""SELECT name from viewtest_relatedview;""")

            with self.assertRaises(Exception):
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
            self.assertEqual(count, 5)

            with self.assertRaises(Exception):
                cur.execute("""SELECT name from viewtest_dependantmaterializedview;""")
                cur.execute("""SELECT name from viewtest_materializedrelatedview; """)

            with self.assertRaises(Exception):
                cur.execute("""SELECT name from viewtest_dependantmaterializedview;""")
