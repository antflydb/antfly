from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_hit import QueryHit


T = TypeVar("T", bound="QueryHits")


@_attrs_define
class QueryHits:
    """A list of query hits.

    Attributes:
        total (Union[Unset, int]): Total number of hits available.
        hits (Union[Unset, list['QueryHit']]):
        max_score (Union[Unset, float]): Maximum score of the results.
    """

    total: Union[Unset, int] = UNSET
    hits: Union[Unset, list["QueryHit"]] = UNSET
    max_score: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total = self.total

        hits: Union[Unset, list[dict[str, Any]]] = UNSET
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

        d = dict(src_dict)
        total = d.pop("total", UNSET)

        hits = []
        _hits = d.pop("hits", UNSET)
        for hits_item_data in _hits or []:
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
