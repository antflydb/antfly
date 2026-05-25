from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="DateRangeStringQuery")


@_attrs_define
class DateRangeStringQuery:
    """
    Attributes:
        start (datetime.datetime | Unset):
        end (datetime.datetime | Unset):
        inclusive_start (bool | None | Unset):
        inclusive_end (bool | None | Unset):
        field (str | Unset):
        boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
            query.
        datetime_parser (str | Unset):
    """

    start: datetime.datetime | Unset = UNSET
    end: datetime.datetime | Unset = UNSET
    inclusive_start: bool | None | Unset = UNSET
    inclusive_end: bool | None | Unset = UNSET
    field: str | Unset = UNSET
    boost: float | None | Unset = UNSET
    datetime_parser: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        start: str | Unset = UNSET
        if not isinstance(self.start, Unset):
            start = self.start.isoformat()

        end: str | Unset = UNSET
        if not isinstance(self.end, Unset):
            end = self.end.isoformat()

        inclusive_start: bool | None | Unset
        if isinstance(self.inclusive_start, Unset):
            inclusive_start = UNSET
        else:
            inclusive_start = self.inclusive_start

        inclusive_end: bool | None | Unset
        if isinstance(self.inclusive_end, Unset):
            inclusive_end = UNSET
        else:
            inclusive_end = self.inclusive_end

        field = self.field

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        datetime_parser = self.datetime_parser

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if start is not UNSET:
            field_dict["start"] = start
        if end is not UNSET:
            field_dict["end"] = end
        if inclusive_start is not UNSET:
            field_dict["inclusive_start"] = inclusive_start
        if inclusive_end is not UNSET:
            field_dict["inclusive_end"] = inclusive_end
        if field is not UNSET:
            field_dict["field"] = field
        if boost is not UNSET:
            field_dict["boost"] = boost
        if datetime_parser is not UNSET:
            field_dict["datetime_parser"] = datetime_parser

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _start = d.pop("start", UNSET)
        start: datetime.datetime | Unset
        if isinstance(_start, Unset):
            start = UNSET
        else:
            start = isoparse(_start)

        _end = d.pop("end", UNSET)
        end: datetime.datetime | Unset
        if isinstance(_end, Unset):
            end = UNSET
        else:
            end = isoparse(_end)

        def _parse_inclusive_start(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        inclusive_start = _parse_inclusive_start(d.pop("inclusive_start", UNSET))

        def _parse_inclusive_end(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        inclusive_end = _parse_inclusive_end(d.pop("inclusive_end", UNSET))

        field = d.pop("field", UNSET)

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        datetime_parser = d.pop("datetime_parser", UNSET)

        date_range_string_query = cls(
            start=start,
            end=end,
            inclusive_start=inclusive_start,
            inclusive_end=inclusive_end,
            field=field,
            boost=boost,
            datetime_parser=datetime_parser,
        )

        date_range_string_query.additional_properties = d
        return date_range_string_query

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
