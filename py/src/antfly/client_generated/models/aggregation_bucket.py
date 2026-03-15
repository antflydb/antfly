from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.aggregation_bucket_sub_aggregations import AggregationBucketSubAggregations


T = TypeVar("T", bound="AggregationBucket")


@_attrs_define
class AggregationBucket:
    """
    Attributes:
        key (str): Bucket key (term, range name, date, etc.)
        doc_count (int): Number of documents in this bucket
        key_as_string (Union[Unset, str]): Formatted key for display (e.g., formatted dates)
        from_ (Union[Unset, float]): Lower bound for range buckets
        to (Union[Unset, float]): Upper bound for range buckets
        from_as_string (Union[Unset, str]): Formatted lower bound
        to_as_string (Union[Unset, str]): Formatted upper bound
        score (Union[Unset, float]): Significance score (for significant_terms)
        bg_count (Union[Unset, int]): Background count (for significant_terms)
        sub_aggregations (Union[Unset, AggregationBucketSubAggregations]): Results of nested sub-aggregations
    """

    key: str
    doc_count: int
    key_as_string: Union[Unset, str] = UNSET
    from_: Union[Unset, float] = UNSET
    to: Union[Unset, float] = UNSET
    from_as_string: Union[Unset, str] = UNSET
    to_as_string: Union[Unset, str] = UNSET
    score: Union[Unset, float] = UNSET
    bg_count: Union[Unset, int] = UNSET
    sub_aggregations: Union[Unset, "AggregationBucketSubAggregations"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        doc_count = self.doc_count

        key_as_string = self.key_as_string

        from_ = self.from_

        to = self.to

        from_as_string = self.from_as_string

        to_as_string = self.to_as_string

        score = self.score

        bg_count = self.bg_count

        sub_aggregations: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.sub_aggregations, Unset):
            sub_aggregations = self.sub_aggregations.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "doc_count": doc_count,
            }
        )
        if key_as_string is not UNSET:
            field_dict["key_as_string"] = key_as_string
        if from_ is not UNSET:
            field_dict["from"] = from_
        if to is not UNSET:
            field_dict["to"] = to
        if from_as_string is not UNSET:
            field_dict["from_as_string"] = from_as_string
        if to_as_string is not UNSET:
            field_dict["to_as_string"] = to_as_string
        if score is not UNSET:
            field_dict["score"] = score
        if bg_count is not UNSET:
            field_dict["bg_count"] = bg_count
        if sub_aggregations is not UNSET:
            field_dict["sub_aggregations"] = sub_aggregations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.aggregation_bucket_sub_aggregations import AggregationBucketSubAggregations

        d = dict(src_dict)
        key = d.pop("key")

        doc_count = d.pop("doc_count")

        key_as_string = d.pop("key_as_string", UNSET)

        from_ = d.pop("from", UNSET)

        to = d.pop("to", UNSET)

        from_as_string = d.pop("from_as_string", UNSET)

        to_as_string = d.pop("to_as_string", UNSET)

        score = d.pop("score", UNSET)

        bg_count = d.pop("bg_count", UNSET)

        _sub_aggregations = d.pop("sub_aggregations", UNSET)
        sub_aggregations: Union[Unset, AggregationBucketSubAggregations]
        if isinstance(_sub_aggregations, Unset):
            sub_aggregations = UNSET
        else:
            sub_aggregations = AggregationBucketSubAggregations.from_dict(_sub_aggregations)

        aggregation_bucket = cls(
            key=key,
            doc_count=doc_count,
            key_as_string=key_as_string,
            from_=from_,
            to=to,
            from_as_string=from_as_string,
            to_as_string=to_as_string,
            score=score,
            bg_count=bg_count,
            sub_aggregations=sub_aggregations,
        )

        aggregation_bucket.additional_properties = d
        return aggregation_bucket

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
