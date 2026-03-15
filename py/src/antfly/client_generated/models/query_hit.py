from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_hit_index_scores import QueryHitIndexScores
    from ..models.query_hit_source import QueryHitSource


T = TypeVar("T", bound="QueryHit")


@_attrs_define
class QueryHit:
    """A single query result hit

    Attributes:
        field_id (str): ID of the record.
        field_score (float): Relevance score of the hit.
        field_index_scores (Union[Unset, QueryHitIndexScores]): Scores partitioned by index when using RRF search.
        field_source (Union[Unset, QueryHitSource]):
        field_sort (Union[Unset, list[str]]): Sort key values for this hit. Pass as search_after or search_before
            to paginate to the next/previous page. Only present when order_by is specified.
    """

    field_id: str
    field_score: float
    field_index_scores: Union[Unset, "QueryHitIndexScores"] = UNSET
    field_source: Union[Unset, "QueryHitSource"] = UNSET
    field_sort: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_id = self.field_id

        field_score = self.field_score

        field_index_scores: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.field_index_scores, Unset):
            field_index_scores = self.field_index_scores.to_dict()

        field_source: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.field_source, Unset):
            field_source = self.field_source.to_dict()

        field_sort: Union[Unset, list[str]] = UNSET
        if not isinstance(self.field_sort, Unset):
            field_sort = self.field_sort

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "_id": field_id,
                "_score": field_score,
            }
        )
        if field_index_scores is not UNSET:
            field_dict["_index_scores"] = field_index_scores
        if field_source is not UNSET:
            field_dict["_source"] = field_source
        if field_sort is not UNSET:
            field_dict["_sort"] = field_sort

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_hit_index_scores import QueryHitIndexScores
        from ..models.query_hit_source import QueryHitSource

        d = dict(src_dict)
        field_id = d.pop("_id")

        field_score = d.pop("_score")

        _field_index_scores = d.pop("_index_scores", UNSET)
        field_index_scores: Union[Unset, QueryHitIndexScores]
        if isinstance(_field_index_scores, Unset):
            field_index_scores = UNSET
        else:
            field_index_scores = QueryHitIndexScores.from_dict(_field_index_scores)

        _field_source = d.pop("_source", UNSET)
        field_source: Union[Unset, QueryHitSource]
        if isinstance(_field_source, Unset):
            field_source = UNSET
        else:
            field_source = QueryHitSource.from_dict(_field_source)

        field_sort = cast(list[str], d.pop("_sort", UNSET))

        query_hit = cls(
            field_id=field_id,
            field_score=field_score,
            field_index_scores=field_index_scores,
            field_source=field_source,
            field_sort=field_sort,
        )

        query_hit.additional_properties = d
        return query_hit

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
