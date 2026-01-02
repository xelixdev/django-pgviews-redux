import pytest

from django_pgviews.exceptions import ConcurrentIndexNotDefinedError
from django_pgviews.refresh import refresh_specific_views
from tests.test_project.viewtest import models


def test_refresh_no_views():
    assert refresh_specific_views([]) == 0


@pytest.mark.django_db
def test_refresh():
    assert models.MaterializedRelatedView.objects.count() == 0, "MaterializedRelatedView should not have anything"
    assert models.DependantMaterializedView.objects.count() == 0, "DependantMaterializedView should not have anything"

    test_model = models.TestModel()
    test_model.name = "Bob"
    test_model.save()

    assert refresh_specific_views([models.MaterializedRelatedView, models.RelatedView]) == 2

    assert models.MaterializedRelatedView.objects.count() == 1
    assert models.DependantMaterializedView.objects.count() == 1


@pytest.mark.django_db
def test_refresh_concurrently():
    with pytest.raises(ConcurrentIndexNotDefinedError):
        assert refresh_specific_views(
            [models.MaterializedRelatedView, models.RelatedView, models.MaterializedRelatedViewWithIndex],
            concurrently=True,
            strict=True,
        )

    assert (
        refresh_specific_views(
            [models.MaterializedRelatedView, models.RelatedView, models.MaterializedRelatedViewWithIndex],
            concurrently=True,
            strict=False,
        )
        == 3
    )
