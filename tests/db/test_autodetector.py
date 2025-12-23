from unittest.mock import patch

from django.db.migrations.state import ProjectState

from django_pgviews.db.migrations.autodetector import PGViewsAutodetector
from django_pgviews.db.migrations.operations import DeleteViewOperation, RegisterViewOperation, ViewState
from django_pgviews.view import MaterializedView, View


class MockView(View):
    class Meta:
        app_label = "test_app"
        db_table = "test_view"


class MockViewChanged(View):
    class Meta:
        app_label = "test_app"
        db_table = "test_view_changed"


def test_detect_new_view():
    from_state = ProjectState()
    to_state = ProjectState()

    autodetector = PGViewsAutodetector(from_state, to_state)
    autodetector.generated_operations = {}

    with patch("django.apps.apps.get_models", return_value=[MockView]):
        autodetector._sort_migrations()

    change = autodetector.generated_operations["test_app"][0]
    assert isinstance(change, RegisterViewOperation)
    assert change.name == "MockView"
    assert change.materialized is False
    assert change.db_name == "test_view"


def test_detect_deleted_view():
    from_state = ProjectState()
    from_state.views = {("test_app", "mockview"): ViewState("test_app", "MockView", False, "test_view")}
    to_state = ProjectState()

    autodetector = PGViewsAutodetector(from_state, to_state)
    autodetector.generated_operations = {}

    with patch("django.apps.apps.get_models", return_value=[]):
        autodetector._sort_migrations()

    change = autodetector.generated_operations["test_app"][0]
    assert isinstance(change, DeleteViewOperation)
    assert change.name == "MockView"
    assert change.materialized is False
    assert change.db_name == "test_view"


def test_detect_changed_view():
    from_state = ProjectState()
    from_state.views = {("test_app", "mockview"): ViewState("test_app", "MockView", False, "test_view")}
    to_state = ProjectState()

    autodetector = PGViewsAutodetector(from_state, to_state)
    autodetector.generated_operations = {}

    class MockViewUpdate(View):
        class Meta:
            app_label = "test_app"
            db_table = "test_view_updated"

    MockViewUpdate.__name__ = "MockView"

    with patch("django.apps.apps.get_models", return_value=[MockViewUpdate]):
        autodetector._sort_migrations()

    changes = autodetector.generated_operations.get("test_app", [])
    assert isinstance(changes[0], DeleteViewOperation)
    assert isinstance(changes[1], RegisterViewOperation)


def test_detect_view_to_materialized_view():
    from_state = ProjectState()
    from_state.views = {("test_app", "mockview"): ViewState("test_app", "MockView", False, "test_view")}
    to_state = ProjectState()

    autodetector = PGViewsAutodetector(from_state, to_state)
    autodetector.generated_operations = {}

    class MockMaterializedView(MaterializedView):
        class Meta:
            app_label = "test_app"
            db_table = "test_view"

    MockMaterializedView.__name__ = "MockView"

    with patch("django.apps.apps.get_models", return_value=[MockMaterializedView]):
        autodetector._sort_migrations()

    changes = autodetector.generated_operations.get("test_app", [])
    assert isinstance(changes[0], DeleteViewOperation)
    assert isinstance(changes[1], RegisterViewOperation)

    assert changes[0].materialized is False
    assert changes[1].materialized is True
