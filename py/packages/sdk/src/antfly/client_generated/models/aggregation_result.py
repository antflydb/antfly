from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.aggregation_bucket import AggregationBucket


T = TypeVar("T", bound="AggregationResult")


@_attrs_define
class AggregationResult:
    """
    Attributes:
        value (float | Unset): Single value for metric aggregations (sum, avg, min, max, count, cardinality)
        count (int | Unset): Document count for stats aggregations
        min_ (float | Unset): Minimum value for stats aggregations
        max_ (float | Unset): Maximum value for stats aggregations
        sum_ (float | Unset): Sum for stats aggregations
        sum_of_squares (float | Unset): Sum of squares for stats aggregations
        avg (float | Unset): Average for stats aggregations
        std_deviation (float | Unset): Standard deviation for stats aggregations
        variance (float | Unset): Variance for stats aggregations
        buckets (list[AggregationBucket] | Unset): Buckets for bucketing aggregations (terms, range, histogram, etc.)
    """

    value: float | Unset = UNSET
    count: int | Unset = UNSET
    min_: float | Unset = UNSET
    max_: float | Unset = UNSET
    sum_: float | Unset = UNSET
    sum_of_squares: float | Unset = UNSET
    avg: float | Unset = UNSET
    std_deviation: float | Unset = UNSET
    variance: float | Unset = UNSET
    buckets: list[AggregationBucket] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        count = self.count

        min_ = self.min_

        max_ = self.max_

        sum_ = self.sum_

        sum_of_squares = self.sum_of_squares

        avg = self.avg

        std_deviation = self.std_deviation

        variance = self.variance

        buckets: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.buckets, Unset):
            buckets = []
            for buckets_item_data in self.buckets:
                buckets_item = buckets_item_data.to_dict()
                buckets.append(buckets_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if value is not UNSET:
            field_dict["value"] = value
        if count is not UNSET:
            field_dict["count"] = count
        if min_ is not UNSET:
            field_dict["min"] = min_
        if max_ is not UNSET:
            field_dict["max"] = max_
        if sum_ is not UNSET:
            field_dict["sum"] = sum_
        if sum_of_squares is not UNSET:
            field_dict["sum_of_squares"] = sum_of_squares
        if avg is not UNSET:
            field_dict["avg"] = avg
        if std_deviation is not UNSET:
            field_dict["std_deviation"] = std_deviation
        if variance is not UNSET:
            field_dict["variance"] = variance
        if buckets is not UNSET:
            field_dict["buckets"] = buckets

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.aggregation_bucket import AggregationBucket

        d = dict(src_dict)
        value = d.pop("value", UNSET)

        count = d.pop("count", UNSET)

        min_ = d.pop("min", UNSET)

        max_ = d.pop("max", UNSET)

        sum_ = d.pop("sum", UNSET)

        sum_of_squares = d.pop("sum_of_squares", UNSET)

        avg = d.pop("avg", UNSET)

        std_deviation = d.pop("std_deviation", UNSET)

        variance = d.pop("variance", UNSET)

        _buckets = d.pop("buckets", UNSET)
        buckets: list[AggregationBucket] | Unset = UNSET
        if _buckets is not UNSET:
            buckets = []
            for buckets_item_data in _buckets:
                buckets_item = AggregationBucket.from_dict(buckets_item_data)

                buckets.append(buckets_item)

        aggregation_result = cls(
            value=value,
            count=count,
            min_=min_,
            max_=max_,
            sum_=sum_,
            sum_of_squares=sum_of_squares,
            avg=avg,
            std_deviation=std_deviation,
            variance=variance,
            buckets=buckets,
        )

        aggregation_result.additional_properties = d
        return aggregation_result

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
