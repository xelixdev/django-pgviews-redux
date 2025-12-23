from __future__ import annotations

__all__ = ["ReadOnlyViewQuerySet"]

from typing import Any, TypeVar

from django.db.models import Model, QuerySet

T = TypeVar("T", bound=Model)


class ReadOnlyViewQuerySet(QuerySet[T]):
    def _raw_delete(self, *args: Any, **kwargs: Any) -> int:
        return 0

    def delete(self) -> tuple[int, dict[str, int]]:
        raise NotImplementedError("Not allowed")

    def update(self, **kwargs: Any) -> int:
        raise NotImplementedError("Not allowed")

    def _update(self, values) -> None:
        raise NotImplementedError("Not allowed")

    def create(self, **kwargs: Any) -> T:
        raise NotImplementedError("Not allowed")

    def update_or_create(self, *args: Any, **kwargs: Any) -> tuple[T, bool]:
        raise NotImplementedError("Not allowed")

    def bulk_create(self, *args: Any, **kwargs: Any) -> list[T]:
        raise NotImplementedError("Not allowed")
