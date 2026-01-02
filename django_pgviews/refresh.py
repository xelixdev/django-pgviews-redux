__all__ = ["refresh_specific_views"]

import logging
from collections.abc import Iterable

from django_pgviews import view as pg
from django_pgviews.dependencies import get_views_dependencies, get_views_dependendants, reorder_by_dependencies

logger = logging.getLogger("django_pgviews.refresh")


def refresh_specific_views(
    to_refresh: Iterable[type[pg.View]],
    concurrently: bool = True,
    strict: bool = False,
) -> int:
    """
    For a specific set of views, refresh all the materialized views, including all dependants and dependencies.

    Returns the number of materialized views refreshed.
    """

    to_refresh = list(to_refresh)

    to_refresh.extend(get_views_dependendants(to_refresh))
    to_refresh.extend(get_views_dependencies(to_refresh))
    to_refresh = reorder_by_dependencies(to_refresh)

    to_refresh_mat_views: list[type[pg.MaterializedView]] = [
        x for x in to_refresh if issubclass(x, pg.MaterializedView)
    ]

    count = 0

    for model in to_refresh_mat_views:
        model.refresh(concurrently=concurrently, strict=strict)

        logger.info(f"Refreshed {model._meta.label}")

        count += 1

    return count
