import pytest

from django_pgviews import view as pg
from django_pgviews.dependencies import get_views_dependencies, get_views_dependendants, reorder_by_dependencies
from tests.test_project.schemadbtest.models import SchemaMonthlyObservationMaterializedView
from tests.test_project.viewtest.models import (
    DependantMaterializedView,
    DependantView,
    MaterializedRelatedView,
    RelatedView,
)


@pytest.mark.parametrize(
    ["views", "results"],
    [
        pytest.param([], [], id="empty"),
        pytest.param([SchemaMonthlyObservationMaterializedView], [], id="no_dependants"),
        pytest.param([MaterializedRelatedView], [DependantMaterializedView], id="mat_view"),
        pytest.param([RelatedView], [DependantView], id="view"),
    ],
)
def test_get_views_dependendants(
    views: list[type[pg.MaterializedView]], results: list[type[pg.MaterializedView]]
) -> None:
    assert get_views_dependendants(views) == results


@pytest.mark.parametrize(
    ["views", "results"],
    [
        pytest.param([], [], id="empty"),
        pytest.param([SchemaMonthlyObservationMaterializedView], [], id="no_dependencies"),
        pytest.param([DependantMaterializedView], [MaterializedRelatedView], id="mat_view"),
        pytest.param([DependantView], [RelatedView], id="view"),
    ],
)
def test_get_views_dependencies(
    views: list[type[pg.MaterializedView]], results: list[type[pg.MaterializedView]]
) -> None:
    assert get_views_dependencies(views) == results


@pytest.mark.parametrize(
    ["views", "results"],
    [
        pytest.param([], [], id="empty"),
        pytest.param(
            [SchemaMonthlyObservationMaterializedView], [SchemaMonthlyObservationMaterializedView], id="no_dependencies"
        ),
        pytest.param([DependantMaterializedView], [DependantMaterializedView], id="mat_view_single"),
        pytest.param([DependantView], [DependantView], id="view_single"),
        pytest.param(
            [MaterializedRelatedView, DependantMaterializedView],
            [MaterializedRelatedView, DependantMaterializedView],
            id="mat_view_correct-order",
        ),
        pytest.param(
            [DependantMaterializedView, MaterializedRelatedView],
            [MaterializedRelatedView, DependantMaterializedView],
            id="mat_view_wrong-order",
        ),
        pytest.param(
            [RelatedView, DependantView],
            [RelatedView, DependantView],
            id="view_correct-order",
        ),
        pytest.param(
            [DependantView, RelatedView],
            [RelatedView, DependantView],
            id="view_wrong-order",
        ),
    ],
)
def test_reorder_by_dependencies(
    views: list[type[pg.MaterializedView]], results: list[type[pg.MaterializedView]]
) -> None:
    assert reorder_by_dependencies(views) == results
