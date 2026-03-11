"""Test Django PGViews."""

import random
import string
from collections.abc import Generator
from contextlib import closing
from datetime import timedelta

import pytest
from django.apps import apps
from django.contrib import auth
from django.contrib.auth.models import User
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connection
from django.db.models.signals import post_migrate
from django.db.utils import DatabaseError, OperationalError
from django.dispatch import receiver
from django.utils import timezone
from pytest_django.fixtures import SettingsWrapper

from django_pgviews.exceptions import ConcurrentIndexNotDefinedError
from django_pgviews.management.operations._utils import _make_where, _schema_and_name
from django_pgviews.management.operations.create_materialized import (
    _concurrent_index_name,
    _get_concurrent_index_tablespace,
    create_materialized_view,
)
from django_pgviews.signals import all_views_synced, view_synced

from . import models

try:
    from psycopg.errors import UndefinedTable
except ImportError:
    from psycopg2.errors import UndefinedTable


def get_list_of_indexes(cursor, cls):
    schema, table = _schema_and_name(cursor.connection, cls._meta.db_table)
    where_fragment, params = _make_where(tablename=table, schemaname=schema)
    cursor.execute(f"SELECT indexname FROM pg_indexes WHERE {where_fragment}", params)
    return {x[0] for x in cursor.fetchall()}


@pytest.mark.django_db
class TestView:
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

    def test_signals(self, settings: SettingsWrapper):
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = False

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

        assert models.LatestSuperusers.objects.count() == 1

    def test_sync_pgviews_materialized_views_check_sql_changed_disabled(self, settings: SettingsWrapper):
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = False

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

    def test_migrate_materialized_views_check_sql_changed_disabled(self, settings: SettingsWrapper):
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = False

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


@pytest.mark.django_db
class TestMaterializedViewsCheckSQLSettings:
    def test_migrate_materialized_views_check_sql_set_to_true_no_data(self, settings: SettingsWrapper) -> None:
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = True

        assert models.TestModel.objects.count() == 0
        assert models.MaterializedRelatedView.objects.count() == 0

        models.TestModel.objects.create(name="Test")
        call_command("migrate")
        assert models.MaterializedRelatedView.objects.count() == 0

        # create it with no data
        with connection.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW viewtest_materializedrelatedview CASCADE;")
            cursor.execute(
                """
                CREATE MATERIALIZED VIEW viewtest_materializedrelatedview as
                SELECT id AS model_id, id FROM viewtest_testmodel
                WITH NO DATA
                """
            )

        # view didn't change but it's not populated, but as it's configured to have data it got refreshed
        call_command("migrate")
        assert models.MaterializedRelatedView.objects.count() == 1

    def test_migrate_materialized_views_check_sql_set_to_true(self, settings: SettingsWrapper) -> None:
        settings.MATERIALIZED_VIEWS_CHECK_SQL_CHANGED = True

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


@pytest.mark.django_db
class TestDependantView:
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


@pytest.mark.django_db
class TestMakeWhere:
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


@pytest.mark.django_db
class TestMaterializedViewSyncDisabledSettings:
    @pytest.fixture(autouse=True)
    def set_up(self, settings: SettingsWrapper):
        # Store original receivers and settings
        _original_receivers = list(post_migrate.receivers)
        _original_config = apps.get_app_config("django_pgviews").counter

        # Clear existing signal receivers
        post_migrate.receivers.clear()

        # Get the app config and reset counter
        config = apps.get_app_config("django_pgviews")
        config.counter = 0

        # Reload app config with new settings
        settings.MATERIALIZED_VIEWS_DISABLE_SYNC_ON_MIGRATE = True
        config.ready()

        # Drop the view if it exists
        with connection.cursor() as cursor:
            cursor.execute("DROP MATERIALIZED VIEW IF EXISTS viewtest_materializedrelatedview CASCADE;")

        yield
        post_migrate.receivers.clear()
        post_migrate.receivers.extend(_original_receivers)
        apps.get_app_config("django_pgviews").counter = _original_config

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


@pytest.mark.django_db(transaction=True)
class TestMaterializedViewTablespace:
    """
    Tests for Meta.db_tablespace and concurrent_index_tablespace support on materialized views.

    These tests require superuser privileges to create and drop tablespaces.
    They are expected to pass when using the 'postgres' superuser configured
    in the test settings.
    """

    @staticmethod
    def _create_pg_tablespace(cursor, name: str) -> None:
        cursor.execute(f"COPY (SELECT 1) TO PROGRAM 'mkdir -p /tmp/{name}';")
        cursor.execute(f"CREATE TABLESPACE {name} LOCATION '/tmp/{name}'")

    @staticmethod
    def _drop_pg_tablespace(cursor, name: str) -> None:
        cursor.execute(f"DROP TABLESPACE IF EXISTS {name};")

    @pytest.fixture
    def tablespace_name(self) -> Generator[str, None, None]:
        rand_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        name = f"test_ts_{rand_suffix}"
        with connection.cursor() as cursor:
            self._create_pg_tablespace(cursor, name)
            yield name
            self._drop_pg_tablespace(cursor, name)

    @pytest.fixture
    def second_tablespace_name(self) -> Generator[str, None, None]:
        rand_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        name = f"test_ts2_{rand_suffix}"
        with connection.cursor() as cursor:
            self._create_pg_tablespace(cursor, name)
            yield name
            self._drop_pg_tablespace(cursor, name)

    @staticmethod
    def _get_index_tablespace(cursor, view_name: str, index_name: str) -> str | None:
        """Returns the tablespace of an existing index (None means the default tablespace).

        Raises AssertionError if the index does not exist in pg_indexes.
        """
        cursor.execute(
            "SELECT tablespace FROM pg_indexes WHERE tablename = %s AND indexname = %s",
            [view_name, index_name],
        )
        row = cursor.fetchone()
        assert row is not None, f"Index {index_name!r} on view {view_name!r} not found in pg_indexes"
        return row[0]

    def test_create_materialized_view_with_tablespace(self, tablespace_name: str) -> None:
        """
        Meta.db_tablespace is applied when creating a materialized view.
        The view should be created in the specified tablespace, which is reflected
        in pg_matviews.tablespace.
        """
        view_name = models.MaterializedRelatedViewWithIndex._meta.db_table

        original_tablespace = models.MaterializedRelatedViewWithIndex._meta.db_tablespace
        try:
            # Patch db_tablespace so the view will be created in our new tablespace.
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = tablespace_name

            result = create_materialized_view(connection, models.MaterializedRelatedViewWithIndex)
            assert result in ("CREATED", "UPDATED")

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT tablespace FROM pg_matviews WHERE matviewname = %s",
                    [view_name],
                )
                row = cursor.fetchone()
                assert row is not None, f"Materialized view {view_name!r} not found in pg_matviews"
                assert row[0] == tablespace_name, f"Expected tablespace {tablespace_name!r}, got {row[0]!r}"

                # The concurrent index does NOT inherit db_tablespace; it uses
                # concurrent_index_tablespace or DEFAULT_INDEX_TABLESPACE instead.
                index_ts = _get_concurrent_index_tablespace(models.MaterializedRelatedViewWithIndex)
                concurrent_name = _concurrent_index_name(view_name, "id", index_ts)
                idx_tablespace = self._get_index_tablespace(cursor, view_name, concurrent_name)
                assert idx_tablespace != tablespace_name, (
                    "Concurrent index should not inherit db_tablespace; use concurrent_index_tablespace instead"
                )
        finally:
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = original_tablespace
            with connection.cursor() as cursor:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

    def test_db_default_tablespace(self, settings: SettingsWrapper, tablespace_name: str) -> None:
        """
        DEFAULT_TABLESPACE is used for the materialized view body when no
        explicit Meta.db_tablespace is set.
        """
        view_name = models.MaterializedRelatedViewWithIndex._meta.db_table

        original_tablespace = models.MaterializedRelatedViewWithIndex._meta.db_tablespace
        try:
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = ""
            settings.DEFAULT_TABLESPACE = tablespace_name

            result = create_materialized_view(connection, models.MaterializedRelatedViewWithIndex)
            assert result in ("CREATED", "UPDATED")

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT tablespace FROM pg_matviews WHERE matviewname = %s",
                    [view_name],
                )
                row = cursor.fetchone()
                assert row is not None, f"Materialized view {view_name!r} not found in pg_matviews"
                assert row[0] == tablespace_name, f"Expected tablespace {tablespace_name!r}, got {row[0]!r}"
        finally:
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = original_tablespace
            with connection.cursor() as cursor:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

    def test_db_default_tablespace_change(
        self, settings: SettingsWrapper, tablespace_name: str, second_tablespace_name: str
    ) -> None:
        """
        Changing DEFAULT_TABLESPACE causes the materialized view to be dropped
        and recreated in the new tablespace when check_sql_changed=True.
        The SQL definition is identical, so this must be detected via tablespace comparison.
        """
        view_name = models.MaterializedRelatedViewWithIndex._meta.db_table

        original_tablespace = models.MaterializedRelatedViewWithIndex._meta.db_tablespace
        try:
            # Rely entirely on DEFAULT_TABLESPACE; no explicit Meta.db_tablespace.
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = ""
            settings.DEFAULT_TABLESPACE = tablespace_name

            result = create_materialized_view(connection, models.MaterializedRelatedViewWithIndex)
            assert result in ("CREATED", "UPDATED")

            with connection.cursor() as cursor:
                cursor.execute("SELECT tablespace FROM pg_matviews WHERE matviewname = %s", [view_name])
                assert cursor.fetchone()[0] == tablespace_name

            # Change DEFAULT_TABLESPACE and sync with check_sql_changed=True.
            settings.DEFAULT_TABLESPACE = second_tablespace_name
            result = create_materialized_view(
                connection, models.MaterializedRelatedViewWithIndex, check_sql_changed=True
            )
            assert result == "UPDATED", "DEFAULT_TABLESPACE changed, view should have been recreated"

            with connection.cursor() as cursor:
                cursor.execute("SELECT tablespace FROM pg_matviews WHERE matviewname = %s", [view_name])
                row = cursor.fetchone()
                assert row is not None, f"Materialized view {view_name!r} not found after recreation"
                assert row[0] == second_tablespace_name, (
                    f"Expected tablespace {second_tablespace_name!r}, got {row[0]!r}"
                )
        finally:
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = original_tablespace
            with connection.cursor() as cursor:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

    def test_concurrent_index_tablespace(self, tablespace_name: str) -> None:
        """
        concurrent_index_tablespace is used for the concurrent unique index.
        When set, the concurrent index should be in that tablespace, independent of
        Meta.db_tablespace.
        """
        view_name = models.MaterializedRelatedViewWithIndex._meta.db_table

        original_index_tablespace = models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace
        try:
            models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace = tablespace_name

            result = create_materialized_view(connection, models.MaterializedRelatedViewWithIndex)
            assert result in ("CREATED", "UPDATED")

            concurrent_name = _concurrent_index_name(view_name, "id", tablespace_name)
            with connection.cursor() as cursor:
                idx_tablespace = self._get_index_tablespace(cursor, view_name, concurrent_name)
                assert idx_tablespace == tablespace_name, (
                    f"Expected concurrent index tablespace {tablespace_name!r}, got {idx_tablespace!r}"
                )
        finally:
            models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace = original_index_tablespace
            with connection.cursor() as cursor:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

    def test_concurrent_index_default_tablespace(self, settings: SettingsWrapper, tablespace_name: str) -> None:
        """
        DEFAULT_INDEX_TABLESPACE is used for the concurrent unique index
        when no explicit concurrent_index_tablespace is set.
        """
        view_name = models.MaterializedRelatedViewWithIndex._meta.db_table

        original_index_tablespace = models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace
        original_default_tablespace = getattr(settings, "DEFAULT_INDEX_TABLESPACE", None)
        try:
            # Clear the explicit concurrent_index_tablespace and set the Django project default.
            models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace = None
            settings.DEFAULT_INDEX_TABLESPACE = tablespace_name

            result = create_materialized_view(connection, models.MaterializedRelatedViewWithIndex)
            assert result in ("CREATED", "UPDATED")

            # The index name includes the tablespace even when it comes from DEFAULT_INDEX_TABLESPACE.
            concurrent_name = _concurrent_index_name(view_name, "id", tablespace_name)
            with connection.cursor() as cursor:
                idx_tablespace = self._get_index_tablespace(cursor, view_name, concurrent_name)
                assert idx_tablespace == tablespace_name, (
                    f"Expected concurrent index tablespace {tablespace_name!r}, got {idx_tablespace!r}"
                )
        finally:
            models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace = original_index_tablespace
            if original_default_tablespace is not None:
                settings.DEFAULT_INDEX_TABLESPACE = original_default_tablespace
            else:
                if hasattr(settings, "DEFAULT_INDEX_TABLESPACE"):
                    delattr(settings, "DEFAULT_INDEX_TABLESPACE")
            with connection.cursor() as cursor:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

    def test_concurrent_index_tablespace_change(self, tablespace_name: str, second_tablespace_name: str) -> None:
        """
        Changing the concurrent_index_tablespace causes the index to be
        dropped and recreated in the new tablespace when check_sql_changed=True.
        """
        view_name = models.MaterializedRelatedViewWithIndex._meta.db_table

        original_index_tablespace = models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace
        try:
            # Create view with concurrent index in the first tablespace.
            models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace = tablespace_name
            result = create_materialized_view(connection, models.MaterializedRelatedViewWithIndex)
            assert result in ("CREATED", "UPDATED")

            concurrent_name_ts1 = _concurrent_index_name(view_name, "id", tablespace_name)
            with connection.cursor() as cursor:
                idx_tablespace = self._get_index_tablespace(cursor, view_name, concurrent_name_ts1)
                assert idx_tablespace == tablespace_name, f"Expected tablespace {tablespace_name}, got {idx_tablespace}"

            # Change to the second tablespace and sync with check_sql_changed=True.
            models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace = second_tablespace_name
            result = create_materialized_view(
                connection, models.MaterializedRelatedViewWithIndex, check_sql_changed=True
            )
            assert result == "EXISTS", "View SQL unchanged, should return EXISTS"

            # The old index (with ts1 name) should be gone, new one (with ts2 name) should exist.
            concurrent_name_ts2 = _concurrent_index_name(view_name, "id", second_tablespace_name)
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT indexname FROM pg_indexes WHERE tablename = %s AND indexname = %s",
                    [view_name, concurrent_name_ts1],
                )
                assert cursor.fetchone() is None, f"Old index {concurrent_name_ts1} should have been dropped"

                idx_tablespace = self._get_index_tablespace(cursor, view_name, concurrent_name_ts2)
                assert idx_tablespace == second_tablespace_name, (
                    f"Expected concurrent index in tablespace {second_tablespace_name}, got {idx_tablespace}"
                )
        finally:
            models.MaterializedRelatedViewWithIndex._concurrent_index_tablespace = original_index_tablespace
            with connection.cursor() as cursor:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

    def test_db_tablespace_change(self, tablespace_name: str, second_tablespace_name: str) -> None:
        """
        Changing Meta.db_tablespace causes the view to be dropped and
        recreated in the new tablespace when check_sql_changed=True.
        The SQL definition is identical, so this must be detected separately.
        """
        view_name = models.MaterializedRelatedViewWithIndex._meta.db_table

        original_tablespace = models.MaterializedRelatedViewWithIndex._meta.db_tablespace
        try:
            # Create view in the first tablespace.
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = tablespace_name
            result = create_materialized_view(connection, models.MaterializedRelatedViewWithIndex)
            assert result in ("CREATED", "UPDATED")

            with connection.cursor() as cursor:
                cursor.execute("SELECT tablespace FROM pg_matviews WHERE matviewname = %s", [view_name])
                assert cursor.fetchone()[0] == tablespace_name

            # Change to the second tablespace and sync with check_sql_changed=True.
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = second_tablespace_name
            result = create_materialized_view(
                connection, models.MaterializedRelatedViewWithIndex, check_sql_changed=True
            )
            assert result == "UPDATED", "Tablespace changed, view should have been recreated"

            with connection.cursor() as cursor:
                cursor.execute("SELECT tablespace FROM pg_matviews WHERE matviewname = %s", [view_name])
                row = cursor.fetchone()
                assert row is not None, f"Materialized view {view_name!r} not found after recreation"
                assert row[0] == second_tablespace_name, (
                    f"Expected tablespace {second_tablespace_name!r}, got {row[0]!r}"
                )
        finally:
            models.MaterializedRelatedViewWithIndex._meta.db_tablespace = original_tablespace
            with connection.cursor() as cursor:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
