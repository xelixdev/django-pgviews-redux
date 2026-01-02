import logging
from typing import Any

from django.apps import apps

from django_pgviews.management.operations.create import create_view
from django_pgviews.management.operations.create_materialized import create_materialized_view
from django_pgviews.signals import all_views_synced, view_synced
from django_pgviews.view import MaterializedView, View

logger = logging.getLogger("django_pgviews.sync_pgviews")
exists_logger = logging.getLogger("django_pgviews.sync_pgviews.exists")


class RunBacklog:
    def __init__(self) -> None:
        super().__init__()
        self.finished: list[str] = []

    def run(self, **kwargs: Any) -> bool:
        self.finished = []

        backlog: list[type[View]] = []
        for view_cls in apps.get_models():
            if not (isinstance(view_cls, type) and issubclass(view_cls, View) and hasattr(view_cls, "sql")):
                continue
            backlog.append(view_cls)
        loop = 0
        while len(backlog) > 0 and loop < 10:
            loop += 1
            backlog = self.run_backlog(backlog, **kwargs)

        if loop >= 10:
            logger.warning("pgviews dependencies hit limit. Check if your model dependencies are correct")
            return False

        return True

    def run_backlog(self, backlog: list[type[View]], **kwargs: Any) -> list[type[View]]:
        raise NotImplementedError


class ViewSyncer(RunBacklog):
    def run(self, **kwargs: Any) -> bool:
        force: bool = kwargs["force"]
        update: bool = kwargs["update"]
        using: str = kwargs["using"]
        materialized_views_check_sql_changed: bool = kwargs.get("materialized_views_check_sql_changed", True)

        if super().run(
            force=force,
            update=update,
            using=using,
            materialized_views_check_sql_changed=materialized_views_check_sql_changed,
        ):
            all_views_synced.send(sender=None, using=using)
            return True
        return False

    def run_backlog(self, backlog: list[type[View]], **kwargs: Any):
        """Installs the list of models given from the previous backlog

        If the correct dependent views have not been installed, the view
        will be added to the backlog.

        Eventually we get to a point where all dependencies are sorted.
        """

        force: bool = kwargs["force"]
        update: bool = kwargs["update"]
        using: str = kwargs["using"]
        materialized_views_check_sql_changed: bool = kwargs["materialized_views_check_sql_changed"]

        new_backlog = []
        for view_cls in backlog:
            skip = False
            name = f"{view_cls._meta.app_label}.{view_cls.__name__}"
            for dep in view_cls._dependencies:
                if dep not in self.finished:
                    skip = True
                    break

            try:
                connection = view_cls.get_view_connection(using=using, restricted_mode=True)
                if not connection:
                    logger.info("Skipping pgview %s (migrations not allowed on %s)", name, using)
                    continue  # Skip

                if skip:
                    new_backlog.append(view_cls)
                    logger.info("Putting pgview at back of queue: %s", name)
                    continue  # Skip

                if isinstance(view_cls(), MaterializedView):
                    status = create_materialized_view(
                        connection, view_cls, check_sql_changed=materialized_views_check_sql_changed
                    )
                else:
                    status = create_view(
                        connection,
                        view_cls._meta.db_table,
                        view_cls.get_sql(),
                        update=update,
                        force=force,
                    )

                view_synced.send(
                    sender=view_cls,
                    update=update,
                    force=force,
                    status=status,
                    has_changed=status not in ("EXISTS", "FORCE_REQUIRED"),
                    using=using,
                )
                self.finished.append(name)
            except Exception as exc:
                exc.view_cls = view_cls  # type: ignore[missing-attribute]
                exc.python_name = name  # type: ignore[missing-attribute]
                raise
            else:
                use_logger = logger

                if status == "CREATED":
                    msg = "created"
                elif status == "UPDATED":
                    msg = "updated"
                elif status == "EXISTS":
                    msg = "already exists, skipping"
                    use_logger = exists_logger
                elif status == "FORCED":
                    msg = "forced overwrite of existing schema"
                elif status == "FORCE_REQUIRED":
                    msg = "exists with incompatible schema, --force required to update"
                else:
                    msg = status

                use_logger.info("pgview %s %s", name, msg)
        return new_backlog


class ViewRefresher(RunBacklog):
    def run_backlog(self, backlog: list[type[View]], **kwargs: Any):
        concurrently: bool = kwargs["concurrently"]
        using: str = kwargs["using"]
        strict: bool = kwargs.get("strict", False)

        new_backlog = []
        for view_cls in backlog:
            skip = False
            name = f"{view_cls._meta.app_label}.{view_cls.__name__}"
            for dep in view_cls._dependencies:
                if dep not in self.finished:
                    skip = True
                    break

            if skip:
                new_backlog.append(view_cls)
                logger.info("Putting pgview at back of queue: %s", name)
                continue  # Skip

            # Don't refresh views which are not associated with this database
            connection = view_cls.get_view_connection(using=using, restricted_mode=True)
            if not connection:
                continue

            if issubclass(view_cls, MaterializedView):
                view_cls.refresh(concurrently=concurrently, strict=strict)
                logger.info("pgview %s refreshed", name)

            self.finished.append(name)

        return new_backlog
