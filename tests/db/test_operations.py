from unittest.mock import MagicMock, patch

import pytest
from django.db.migrations.optimizer import MigrationOptimizer
from django.db.migrations.state import ProjectState

from django_pgviews.db.migrations.operations import DeleteViewOperation, RegisterViewOperation, ViewOperation, ViewState
from django_pgviews.view import MaterializedView, View


class OpMockView(View):
    class Meta:
        app_label = "test_app"
        db_table = "test_view"


class OpMockMaterializedView(MaterializedView):
    class Meta:
        app_label = "test_app"
        db_table = "test_materialized_view"


def test_view_state_from_view():
    state = ViewState.from_view(OpMockView)
    assert state.app_label == "test_app"
    assert state.name == "OpMockView"
    assert not state.materialized
    assert state.db_name == "test_view"


def test_view_state_from_materialized_view():
    state = ViewState.from_view(OpMockMaterializedView)
    assert state.app_label == "test_app"
    assert state.name == "OpMockMaterializedView"
    assert state.materialized
    assert state.db_name == "test_materialized_view"


def test_view_state_equality():
    state1 = ViewState("test_app", "View", False, "view")
    state2 = ViewState("test_app", "View", False, "view")
    state3 = ViewState("test_app", "View", True, "view")
    assert state1 == state2
    assert state1 != state3


def test_register_view_operation_state_forwards():
    operation = RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view")
    state = ProjectState()
    operation.state_forwards("test_app", state)

    assert hasattr(state, "views")
    assert ("test_app", "opmockview") in state.views
    view_state = state.views[("test_app", "opmockview")]
    assert view_state.name == "OpMockView"
    assert view_state.db_name == "test_view"
    assert not view_state.materialized


def test_delete_view_operation_state_forwards():
    state = ProjectState()
    state.views = {("test_app", "opmockview"): ViewState("test_app", "OpMockView", False, "test_view")}

    operation = DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view")
    operation.state_forwards("test_app", state)
    assert ("test_app", "opmockview") not in state.views


@pytest.fixture()
def schema_editor() -> MagicMock:
    schema_editor = MagicMock()
    schema_editor.connection.alias = "default"
    return schema_editor


def test_delete_view_operation_database_forwards(schema_editor: MagicMock):
    operation = DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view")

    with patch("django_pgviews.db.migrations.operations.clear_view") as mock_clear_view:
        operation.database_forwards("test_app", schema_editor, None, None)
        mock_clear_view.assert_called_once()
        args, kwargs = mock_clear_view.call_args
        # clear_view(connection, self.db_name, materialized=self.materialized)
        assert args[1] == "test_view"
        assert kwargs["materialized"] is False


def test_delete_view_operation_database_forwards_materialized(schema_editor: MagicMock):
    operation = DeleteViewOperation(name="OpMockMaterializedView", materialized=True, db_name="test_materialized_view")

    with patch("django_pgviews.db.migrations.operations.clear_view") as mock_clear_view:
        operation.database_forwards("test_app", schema_editor, None, None)
        mock_clear_view.assert_called_once()
        args, kwargs = mock_clear_view.call_args
        # clear_view(connection, self.db_name, materialized=self.materialized)
        assert args[1] == "test_materialized_view"
        assert kwargs["materialized"] is True


@pytest.mark.parametrize(
    ["operations", "expected"],
    [
        pytest.param(
            [
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                RegisterViewOperation(name="OpMockMatView", materialized=True, db_name="test_mat_view"),
            ],
            None,
            id="two_different_views",
        ),
        pytest.param(
            [
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
            ],
            [
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
            ],
            id="two_identical_create",
        ),
        pytest.param(
            [
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
            ],
            [
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
            ],
            id="two_identical_delete",
        ),
        pytest.param(
            [
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
            ],
            [],
            id="delete_identical",
        ),
        pytest.param(
            [
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
            ],
            [
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
            ],
            id="register_identical",
        ),
        pytest.param(
            [
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                RegisterViewOperation(name="OpMockView", materialized=True, db_name="test_view"),
            ],
            [
                RegisterViewOperation(name="OpMockView", materialized=True, db_name="test_view"),
            ],
            id="change_materialized",
        ),
        pytest.param(
            [
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                DeleteViewOperation(name="OpMockView", materialized=False, db_name="test_view"),
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view_changed"),
            ],
            [
                RegisterViewOperation(name="OpMockView", materialized=False, db_name="test_view_changed"),
            ],
            id="change_db_name",
        ),
    ],
)
def test_optimize(operations: list[ViewOperation], expected: list[ViewOperation] | None):
    if expected is None:
        expected = operations
    optimized = MigrationOptimizer().optimize(operations, "test_app")
    assert len(optimized) == len(expected)

    for op, exp_op in zip(optimized, expected, strict=True):
        assert op.name == exp_op.name
        assert op.materialized == exp_op.materialized
        assert op.db_name == exp_op.db_name
