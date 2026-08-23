from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_hit import QueryHit
    from ..models.query_hits_total import QueryHitsTotal


T = TypeVar("T", bound="QueryHits")


@_attrs_define
class QueryHits:
    """A list of query hits.

    Attributes:
        total (QueryHitsTotal | Unset): Total hit count metadata.
        hits (list[QueryHit] | Unset):
        max_score (float | Unset): Best relevance score among the returned results. Scores are always higher-is-better.
    """

    total: QueryHitsTotal | Unset = UNSET
    hits: list[QueryHit] | Unset = UNSET
    max_score: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total: dict[str, Any] | Unset = UNSET
        if not isinstance(self.total, Unset):
            total = self.total.to_dict()

        hits: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.hits, Unset):
            hits = []
            for hits_item_data in self.hits:
                hits_item = hits_item_data.to_dict()
                hits.append(hits_item)

        max_score = self.max_score

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if total is not UNSET:
            field_dict["total"] = total
        if hits is not UNSET:
            field_dict["hits"] = hits
        if max_score is not UNSET:
            field_dict["max_score"] = max_score

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_hit import QueryHit
        from ..models.query_hits_total import QueryHitsTotal

        d = dict(src_dict)
        _total = d.pop("total", UNSET)
        total: QueryHitsTotal | Unset
        if isinstance(_total, Unset):
            total = UNSET
        else:
            total = QueryHitsTotal.from_dict(_total)

        _hits = d.pop("hits", UNSET)
        hits: list[QueryHit] | Unset = UNSET
        if _hits is not UNSET:
            hits = []
            for hits_item_data in _hits:
                hits_item = QueryHit.from_dict(hits_item_data)

                hits.append(hits_item)

        max_score = d.pop("max_score", UNSET)

        query_hits = cls(
            total=total,
            hits=hits,
            max_score=max_score,
        )

        query_hits.additional_properties = d
        return query_hits

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
