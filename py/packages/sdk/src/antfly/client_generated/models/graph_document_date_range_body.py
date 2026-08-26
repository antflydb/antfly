from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphDocumentDateRangeBody")


@_attrs_define
class GraphDocumentDateRangeBody:
    """At least one of start or end is required and enforced by every Antfly execution boundary. Bounds are RFC 3339
    instants in Antfly's unsigned Unix-nanosecond domain, from 1970-01-01T00:00:00Z through
    2554-07-21T23:34:33.709551615Z inclusive.

        Attributes:
            path (str): RFC 6901 JSON Pointer to the stored-document value.
            start (datetime.datetime | Unset):
            end (datetime.datetime | Unset):
            inclusive_start (bool | Unset):  Default: True.
            inclusive_end (bool | Unset):  Default: False.
    """

    path: str
    start: datetime.datetime | Unset = UNSET
    end: datetime.datetime | Unset = UNSET
    inclusive_start: bool | Unset = True
    inclusive_end: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        path = self.path

        start: str | Unset = UNSET
        if not isinstance(self.start, Unset):
            start = self.start.isoformat()

        end: str | Unset = UNSET
        if not isinstance(self.end, Unset):
            end = self.end.isoformat()

        inclusive_start = self.inclusive_start

        inclusive_end = self.inclusive_end

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "path": path,
            }
        )
        if start is not UNSET:
            field_dict["start"] = start
        if end is not UNSET:
            field_dict["end"] = end
        if inclusive_start is not UNSET:
            field_dict["inclusive_start"] = inclusive_start
        if inclusive_end is not UNSET:
            field_dict["inclusive_end"] = inclusive_end

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        path = d.pop("path")

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

        inclusive_start = d.pop("inclusive_start", UNSET)

        inclusive_end = d.pop("inclusive_end", UNSET)

        graph_document_date_range_body = cls(
            path=path,
            start=start,
            end=end,
            inclusive_start=inclusive_start,
            inclusive_end=inclusive_end,
        )

        return graph_document_date_range_body
