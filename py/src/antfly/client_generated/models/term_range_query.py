from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TermRangeQuery")


@_attrs_define
class TermRangeQuery:
    """
    Attributes:
        min_ (Union[None, Unset, str]):
        max_ (Union[None, Unset, str]):
        inclusive_min (Union[None, Unset, bool]):
        inclusive_max (Union[None, Unset, bool]):
        field (Union[Unset, str]):
        boost (Union[None, Unset, float]): A floating-point number used to decrease or increase the relevance scores of
            a query.
    """

    min_: Union[None, Unset, str] = UNSET
    max_: Union[None, Unset, str] = UNSET
    inclusive_min: Union[None, Unset, bool] = UNSET
    inclusive_max: Union[None, Unset, bool] = UNSET
    field: Union[Unset, str] = UNSET
    boost: Union[None, Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        min_: Union[None, Unset, str]
        if isinstance(self.min_, Unset):
            min_ = UNSET
        else:
            min_ = self.min_

        max_: Union[None, Unset, str]
        if isinstance(self.max_, Unset):
            max_ = UNSET
        else:
            max_ = self.max_

        inclusive_min: Union[None, Unset, bool]
        if isinstance(self.inclusive_min, Unset):
            inclusive_min = UNSET
        else:
            inclusive_min = self.inclusive_min

        inclusive_max: Union[None, Unset, bool]
        if isinstance(self.inclusive_max, Unset):
            inclusive_max = UNSET
        else:
            inclusive_max = self.inclusive_max

        field = self.field

        boost: Union[None, Unset, float]
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if min_ is not UNSET:
            field_dict["min"] = min_
        if max_ is not UNSET:
            field_dict["max"] = max_
        if inclusive_min is not UNSET:
            field_dict["inclusive_min"] = inclusive_min
        if inclusive_max is not UNSET:
            field_dict["inclusive_max"] = inclusive_max
        if field is not UNSET:
            field_dict["field"] = field
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_min_(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        min_ = _parse_min_(d.pop("min", UNSET))

        def _parse_max_(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        max_ = _parse_max_(d.pop("max", UNSET))

        def _parse_inclusive_min(data: object) -> Union[None, Unset, bool]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, bool], data)

        inclusive_min = _parse_inclusive_min(d.pop("inclusive_min", UNSET))

        def _parse_inclusive_max(data: object) -> Union[None, Unset, bool]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, bool], data)

        inclusive_max = _parse_inclusive_max(d.pop("inclusive_max", UNSET))

        field = d.pop("field", UNSET)

        def _parse_boost(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        boost = _parse_boost(d.pop("boost", UNSET))

        term_range_query = cls(
            min_=min_,
            max_=max_,
            inclusive_min=inclusive_min,
            inclusive_max=inclusive_max,
            field=field,
            boost=boost,
        )

        term_range_query.additional_properties = d
        return term_range_query

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
