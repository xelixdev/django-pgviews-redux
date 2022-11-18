import logging

from django.apps import apps

from django_pgviews.signals import view_synced, all_views_synced
from django_pgviews.view import create_view, View, MaterializedView, create_materialized_view

logger = logging.getLogger("django_pgviews.sync_pgviews")
exists_logger = logging.getLogger("django_pgviews.sync_pgviews.exists")


class RunBacklog(object):
    def __init__(self) -> None:
        super().__init__()
        self.finished = []

    def run(self, **kwargs):
        self.finished = []
        backlog = []
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

    def run_backlog(self, backlog, **kwargs):
        raise NotImplementedError


class ViewSyncer(RunBacklog):
    def run(self, force, update, using, materialized_views_check_sql_changed=False, **options):
        if super().run(
            force=force,
            update=update,
            using=using,
            materialized_views_check_sql_changed=materialized_views_check_sql_changed,
        ):
            all_views_synced.send(sender=None, using=using)

    def run_backlog(self, backlog, *, force, update, using, materialized_views_check_sql_changed, **kwargs):
        """Installs the list of models given from the previous backlog

        If the correct dependent views have not been installed, the view
        will be added to the backlog.

        Eventually we get to a point where all dependencies are sorted.
        """
        new_backlog = []
        for view_cls in backlog:
            skip = False
            name = "{}.{}".format(view_cls._meta.app_label, view_cls.__name__)
            for dep in view_cls._dependencies:
                if dep not in self.finished:
                    skip = True
                    break

            if skip is True:
                new_backlog.append(view_cls)
                logger.info("Putting pgview at back of queue: %s", name)
                continue  # Skip

            try:
                connection = view_cls.get_view_connection(using=using, restricted_mode=True)
                if not connection:
                    logger.info("Skipping pgview %s (migrations not allowed on %s)", name, using)
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
                exc.view_cls = view_cls
                exc.python_name = name
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
    def run(self, concurrently, using, **kwargs):
        return super().run(concurrently=concurrently, using=using, **kwargs)

    def run_backlog(self, backlog, *, concurrently, using, **kwargs):
        new_backlog = []
        for view_cls in backlog:
            skip = False
            name = "{}.{}".format(view_cls._meta.app_label, view_cls.__name__)
            for dep in view_cls._dependencies:
                if dep not in self.finished:
                    skip = True
                    break

            if skip is True:
                new_backlog.append(view_cls)
                logger.info("Putting pgview at back of queue: %s", name)
                continue  # Skip

            # Don't refresh views not associated with this database
            connection = view_cls.get_view_connection(using=using, restricted_mode=True)
            if not connection:
                continue

            if issubclass(view_cls, MaterializedView):
                view_cls.refresh(concurrently=concurrently)
                logger.info("pgview %s refreshed", name)

            self.finished.append(name)

        return new_backlog
