from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_hit_hierarchy import QueryHitHierarchy
    from ..models.query_hit_index_scores import QueryHitIndexScores
    from ..models.query_hit_source import QueryHitSource
    from ..models.query_score_details import QueryScoreDetails


T = TypeVar("T", bound="QueryHit")


@_attrs_define
class QueryHit:
    """A single query result hit

    Attributes:
        field_id (str): ID of the record.
        field_score (float): Relevance score of the hit, normalized so higher values always rank first.
        field_distance (float | Unset): Raw vector distance for direct dense-vector hits; lower values are better.
            For a source group ranked by dense descendants, this is the distance of
            the best matching descendant that supplied the group score. Omitted for
            non-dense and fused results.
        field_index_scores (QueryHitIndexScores | Unset): Scores partitioned by index when using RRF search.
        field_score_details (QueryScoreDetails | Unset): Optional score provenance for ranking features that changed the
            final hit score.
        field_source (QueryHitSource | Unset):
        hierarchy (QueryHitHierarchy | Unset):
        field_sort (list[Any] | Unset): Sort key values for this hit. Pass as search_after or search_before
            to paginate to the next/previous page. Values preserve their JSON
            types. Present for ordered result pages, including cursor-only
            requests whose effective order is `_id` ascending.
    """

    field_id: str
    field_score: float
    field_distance: float | Unset = UNSET
    field_index_scores: QueryHitIndexScores | Unset = UNSET
    field_score_details: QueryScoreDetails | Unset = UNSET
    field_source: QueryHitSource | Unset = UNSET
    hierarchy: QueryHitHierarchy | Unset = UNSET
    field_sort: list[Any] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_id = self.field_id

        field_score = self.field_score

        field_distance = self.field_distance

        field_index_scores: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_index_scores, Unset):
            field_index_scores = self.field_index_scores.to_dict()

        field_score_details: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_score_details, Unset):
            field_score_details = self.field_score_details.to_dict()

        field_source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_source, Unset):
            field_source = self.field_source.to_dict()

        hierarchy: dict[str, Any] | Unset = UNSET
        if not isinstance(self.hierarchy, Unset):
            hierarchy = self.hierarchy.to_dict()

        field_sort: list[Any] | Unset = UNSET
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
        if field_distance is not UNSET:
            field_dict["_distance"] = field_distance
        if field_index_scores is not UNSET:
            field_dict["_index_scores"] = field_index_scores
        if field_score_details is not UNSET:
            field_dict["_score_details"] = field_score_details
        if field_source is not UNSET:
            field_dict["_source"] = field_source
        if hierarchy is not UNSET:
            field_dict["hierarchy"] = hierarchy
        if field_sort is not UNSET:
            field_dict["_sort"] = field_sort

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_hit_hierarchy import QueryHitHierarchy
        from ..models.query_hit_index_scores import QueryHitIndexScores
        from ..models.query_hit_source import QueryHitSource
        from ..models.query_score_details import QueryScoreDetails

        d = dict(src_dict)
        field_id = d.pop("_id")

        field_score = d.pop("_score")

        field_distance = d.pop("_distance", UNSET)

        _field_index_scores = d.pop("_index_scores", UNSET)
        field_index_scores: QueryHitIndexScores | Unset
        if isinstance(_field_index_scores, Unset):
            field_index_scores = UNSET
        else:
            field_index_scores = QueryHitIndexScores.from_dict(_field_index_scores)

        _field_score_details = d.pop("_score_details", UNSET)
        field_score_details: QueryScoreDetails | Unset
        if isinstance(_field_score_details, Unset):
            field_score_details = UNSET
        else:
            field_score_details = QueryScoreDetails.from_dict(_field_score_details)

        _field_source = d.pop("_source", UNSET)
        field_source: QueryHitSource | Unset
        if isinstance(_field_source, Unset):
            field_source = UNSET
        else:
            field_source = QueryHitSource.from_dict(_field_source)

        _hierarchy = d.pop("hierarchy", UNSET)
        hierarchy: QueryHitHierarchy | Unset
        if isinstance(_hierarchy, Unset):
            hierarchy = UNSET
        else:
            hierarchy = QueryHitHierarchy.from_dict(_hierarchy)

        field_sort = cast(list[Any], d.pop("_sort", UNSET))

        query_hit = cls(
            field_id=field_id,
            field_score=field_score,
            field_distance=field_distance,
            field_index_scores=field_index_scores,
            field_score_details=field_score_details,
            field_source=field_source,
            hierarchy=hierarchy,
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
