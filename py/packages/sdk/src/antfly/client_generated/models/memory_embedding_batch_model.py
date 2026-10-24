from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEmbeddingBatchModel")


@_attrs_define
class MemoryEmbeddingBatchModel:
    """
    Attributes:
        provider (str):
        model (str):
        revision (str):
        dimensions (int | None | Unset):
    """

    provider: str
    model: str
    revision: str
    dimensions: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider

        model = self.model

        revision = self.revision

        dimensions: int | None | Unset
        if isinstance(self.dimensions, Unset):
            dimensions = UNSET
        else:
            dimensions = self.dimensions

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "provider": provider,
                "model": model,
                "revision": revision,
            }
        )
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = d.pop("provider")

        model = d.pop("model")

        revision = d.pop("revision")

        def _parse_dimensions(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        dimensions = _parse_dimensions(d.pop("dimensions", UNSET))

        memory_embedding_batch_model = cls(
            provider=provider,
            model=model,
            revision=revision,
            dimensions=dimensions,
        )

        return memory_embedding_batch_model
