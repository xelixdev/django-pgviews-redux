from django.apps import apps
from django.db.migrations.autodetector import MigrationAutodetector

from django_pgviews.db.migrations.operations import DeleteViewOperation, RegisterViewOperation, ViewState
from django_pgviews.view import View


class PGViewsAutodetector(MigrationAutodetector):
    def populate_to_state_views(self):
        """
        Ideally, we'd override ProjectState.from_apps() to do this, but
        the makemigrations command does not expose a nice way to generate a ProjectState, so we need to hack it in.
        """
        app_views = {}
        for model in apps.get_models():
            if not issubclass(model, View):
                continue
            view_state = ViewState.from_view(model)
            app_views[(view_state.app_label, view_state.name_lower)] = view_state

        self.to_state.views.update(app_views)

    def _sort_migrations(self):
        """
        Detects new/deleted views.

        Ideally we'd override `_detect_changes`, but we need
        1. Run after self.generated_operations is created
        2. Run before self._sort_migrations()

        This is probably the easiest way to do this.
        """
        # ideally we'd subclass ProjectState but we can't
        if not hasattr(self.from_state, "views"):
            self.from_state.views = {}
        if not hasattr(self.to_state, "views"):
            self.to_state.views = {}

        self.populate_to_state_views()

        self.old_view_state = {}
        self.new_view_state = {}

        for (app_label, view_name), view_state in self.from_state.views.items():
            self.old_view_state[(app_label, view_name)] = view_state
        for (app_label, view_name), view_state in self.to_state.views.items():
            self.new_view_state[(app_label, view_name)] = view_state

        self.generate_deleted_views()
        self.generate_created_views()

        return super()._sort_migrations()

    def generate_deleted_views(self):
        new_keys = self.new_view_state.keys()
        deleted_views = self.old_view_state.keys() - new_keys

        for key in self.new_view_state.keys() & self.old_view_state.keys():
            if self.new_view_state[key] != self.old_view_state[key]:
                deleted_views.add(key)

        for app_label, view_name in deleted_views:
            view_state = self.from_state.views[app_label, view_name]

            self.add_operation(
                app_label,
                DeleteViewOperation(
                    name=view_state.name,
                    materialized=view_state.materialized,
                    db_name=view_state.db_name,
                ),
            )

    def generate_created_views(self):
        old_keys = self.old_view_state.keys()
        added_views = self.new_view_state.keys() - old_keys

        for key in self.new_view_state.keys() & self.old_view_state.keys():
            if self.new_view_state[key] != self.old_view_state[key]:
                added_views.add(key)

        for app_label, view_name in added_views:
            view_state = self.to_state.views[app_label, view_name]

            self.add_operation(
                app_label,
                RegisterViewOperation(
                    name=view_state.name,
                    materialized=view_state.materialized,
                    db_name=view_state.db_name,
                ),
            )
