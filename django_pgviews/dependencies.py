__all__ = ["get_views_dependencies", "get_views_dependendants", "reorder_by_dependencies"]

from collections.abc import Iterable

from django.apps import apps

from django_pgviews import view as pg


def get_views_dependendants(mat_views: Iterable[type[pg.View]]) -> list[type[pg.View]]:
    """
    Returns a list of materialized views which depend on the given materialized views.
    """
    set_mat_views = set(mat_views)
    depending_views = []

    for model in apps.get_models():  # for all models
        if not issubclass(model, pg.View):  # not a mat view
            continue

        if model in set_mat_views:  # already being refreshed
            continue

        # dependencies are defined as strings (app.model), so comparing the model label
        if any(view._meta.label in getattr(model, "_dependencies", []) for view in mat_views) or any(
            view._meta.label in getattr(parent, "_dependencies", [])
            for view in mat_views
            for parent in model.__bases__  # parent classes
        ):
            depending_views.append(model)

    return depending_views


def get_views_dependencies(mat_views: Iterable[type[pg.View]]) -> list[type[pg.View]]:
    """
    Returns a list of materialized views on which the given materialized views depend.
    """
    depending_on_views = set()

    while True:
        added = 0
        set_mat_views = set(mat_views) | depending_on_views
        for model in set_mat_views:
            for dependency in getattr(model, "_dependencies", []):
                dependency_app, dependency_model_name = dependency.split(".")

                app = apps.get_app_config(dependency_app)
                dependency_model = getattr(app.models_module, dependency_model_name)

                if dependency_model not in set_mat_views:
                    depending_on_views.add(dependency_model)
                    added += 1

        if added == 0:
            break

    return list(depending_on_views)


def reorder_by_dependencies(to_refresh: Iterable[type[pg.View]]) -> list[type[pg.View]]:
    """
    Reorders the list of materialized views to refresh by dependencies, so views are refreshed in the correct order.
    """
    to_refresh_strings = {a._meta.label for a in to_refresh}
    left_to_sort = set(to_refresh)
    ordered_correctly: list[type[pg.View]] = []

    def dependency_already_in(x: str) -> bool:
        return x not in to_refresh_strings or x in {a._meta.label for a in ordered_correctly}

    i = 0

    while left_to_sort:
        this_round: list[type[pg.View]] = []
        for view in left_to_sort:
            if all(dependency_already_in(dependency) for dependency in getattr(view, "_dependencies", [])) and all(
                dependency_already_in(dependency)
                for parent in view.__bases__  # parent classes
                for dependency in getattr(parent, "_dependencies", [])
            ):
                this_round.append(view)

        ordered_correctly += sorted(this_round, key=lambda x: x._meta.label)  # to make testing nicer
        for view in this_round:  # to not update the set while iterating
            left_to_sort.remove(view)

        i += 1

        if i >= 10:
            msg = "Endless loop!"
            raise ValueError(msg)

    return ordered_correctly
