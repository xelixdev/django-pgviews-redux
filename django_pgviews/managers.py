from __future__ import annotations

__all__ = ["ReadOnlyViewManager"]


from django.db import models

from django_pgviews.querysets import ReadOnlyViewQuerySet


class ReadOnlyViewManager(models.Manager):
    def get_queryset(self):
        return ReadOnlyViewQuerySet(self.model, using=self._db)
