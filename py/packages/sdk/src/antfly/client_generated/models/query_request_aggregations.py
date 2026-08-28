from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.aggregation_request import AggregationRequest


T = TypeVar("T", bound="QueryRequestAggregations")


@_attrs_define
class QueryRequestAggregations:
    """Aggregation requests for computing metrics and bucketing results.
    Each key is a user-defined name for the aggregation, and the value specifies the aggregation configuration.

    When `hierarchy.group_by` is present, aggregations operate on the complete
    set of top-level grouped source or unit records. Nested `group_by.matches` are
    bounded evidence projections and are not counted as aggregation rows.

    Supports metric aggregations (sum, avg, min, max, count, stats, cardinality),
    bucketing aggregations (terms, range, date_range, histogram, date_histogram),
    geo aggregations (geohash_grid, geo_distance), and analytics (significant_terms).

    Example:
    ```json
    {
      "price_stats": {
        "type": "stats",
        "field": "price"
      },
      "categories": {
        "type": "terms",
        "field": "category",
        "size": 10
      }
    }
    ```

    """

    additional_properties: dict[str, AggregationRequest] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.aggregation_request import AggregationRequest

        d = dict(src_dict)
        query_request_aggregations = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = AggregationRequest.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        query_request_aggregations.additional_properties = additional_properties
        return query_request_aggregations

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> AggregationRequest:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: AggregationRequest) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
