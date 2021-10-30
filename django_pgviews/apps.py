import logging

from django import apps
from django.db.models import signals

logger = logging.getLogger("django_pgviews.sync_pgviews")


class ViewConfig(apps.AppConfig):
    """
    The base configuration for Django PGViews. We use this to setup our
    post_migrate signal handlers.
    """

    counter = 0
    name = "django_pgviews"
    verbose_name = "Django Postgres Views"

    def sync_pgviews(self, sender, app_config, using, **kwargs):
        """
        Forcibly sync the views.
        """
        self.counter = self.counter + 1
        total = len([a for a in apps.apps.get_app_configs() if a.models_module is not None])

        if self.counter == total:
            logger.info("All applications have migrated, time to sync")
            # Import here otherwise Django doesn't start properly
            # (models in app init are not allowed)
            from .models import ViewSyncer
            from django.conf import settings

            vs = ViewSyncer()
            vs.run(
                force=True,
                update=True,
                materialized_views_check_sql_changed=getattr(settings, "MATERIALIZED_VIEWS_CHECK_SQL_CHANGED", False),
                using=using,
            )
            self.counter = 0

    def ready(self):
        """
        Find and setup the apps to set the post_migrate hooks for.
        """
        signals.post_migrate.connect(self.sync_pgviews)
