from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EmbeddingsIndexStatsEnrichmentRuntime")


@_attrs_define
class EmbeddingsIndexStatsEnrichmentRuntime:
    """Embedding enrichment worker runtime diagnostics.

    Attributes:
        pending_sequence_count (int | Unset):
    """

    pending_sequence_count: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        pending_sequence_count = self.pending_sequence_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if pending_sequence_count is not UNSET:
            field_dict["pending_sequence_count"] = pending_sequence_count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        pending_sequence_count = d.pop("pending_sequence_count", UNSET)

        embeddings_index_stats_enrichment_runtime = cls(
            pending_sequence_count=pending_sequence_count,
        )

        embeddings_index_stats_enrichment_runtime.additional_properties = d
        return embeddings_index_stats_enrichment_runtime

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
