from typing import Any

from django.db import connections, router
from django.db.migrations.operations.base import Operation, OperationCategory
from django.db.migrations.state import ProjectState

from django_pgviews.management.operations.clear import clear_view
from django_pgviews.view import MaterializedView, View


class ViewState:
    def __init__(self, app_label: str, name: str, materialized: bool, db_name: str) -> None:
        self.app_label = app_label
        self.name = name
        self.materialized = materialized
        self.db_name = db_name

    @property
    def name_lower(self):
        return self.name.lower()

    @classmethod
    def from_view(cls, view: type[View]):
        return cls(
            app_label=view._meta.app_label,
            name=view.__name__,
            materialized=issubclass(view, MaterializedView),
            db_name=view._meta.db_table,
        )

    def __eq__(self, value, /):
        return self.__dict__ == value.__dict__


class ViewOperation(Operation):
    reversible = False
    reduces_to_sql = True
    elidable = False

    serialization_expand_args = ["name", "materialized", "db_name"]

    def __init__(self, name: str, materialized: bool, db_name: str) -> None:
        self.name = name
        self.materialized = materialized
        self.db_name = db_name
        super().__init__()

    def deconstruct(self) -> tuple[str, list[str], dict[str, Any]]:
        kwargs = {
            "name": self.name,
            "materialized": self.materialized,
            "db_name": self.db_name,
        }
        return self.__class__.__qualname__, [], kwargs

    @property
    def name_lower(self):
        return self.name.lower()


class RegisterViewOperation(ViewOperation):
    category = OperationCategory.ADDITION

    def describe(self):
        if self.materialized:
            return f"Register materialized view {self.name}"
        return f"Register view {self.name}"

    def state_forwards(self, app_label: str, state: ProjectState) -> None:
        if not hasattr(state, "views"):
            state.views = {}

        view_state = ViewState(
            app_label,
            self.name,
            materialized=self.materialized,
            db_name=self.db_name,
        )

        model_key = view_state.app_label, view_state.name_lower
        state.views[model_key] = view_state

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        pass

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        pass

    def reduce(self, operation: Operation, app_label: str) -> None:
        if (
            not isinstance(operation, (DeleteViewOperation, RegisterViewOperation))
            or self.name_lower != operation.name_lower
            or self.materialized != operation.materialized
            or self.db_name != operation.db_name
        ):
            return False

        if isinstance(operation, RegisterViewOperation):
            return [operation]
        return []


class DeleteViewOperation(ViewOperation):
    category = OperationCategory.REMOVAL

    def describe(self):
        if self.materialized:
            return f"Delete materialized view {self.name}"
        return f"Delete view {self.name}"

    def state_forwards(self, app_label: str, state: ProjectState) -> None:
        if not hasattr(state, "views"):
            state.views = {}

        model_key = app_label, self.name_lower
        state.views.pop(model_key, None)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if not router.allow_migrate(schema_editor.connection.alias, app_label):
            return

        connection = connections[schema_editor.connection.alias]

        clear_view(connection, self.db_name, materialized=self.materialized)

    def reduce(self, operation: Operation, app_label: str) -> None:
        if (
            not isinstance(operation, (DeleteViewOperation, RegisterViewOperation))
            or self.name_lower != operation.name_lower
            or self.materialized != operation.materialized
            or self.db_name != operation.db_name
        ):
            return False

        if isinstance(operation, DeleteViewOperation):
            return [operation]
        return False
