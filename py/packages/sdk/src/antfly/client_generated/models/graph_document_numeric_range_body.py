from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphDocumentNumericRangeBody")


@_attrs_define
class GraphDocumentNumericRangeBody:
    """At least one of min or max is required and enforced by the server.

    Attributes:
        field (str):
        min_ (float | Unset):
        max_ (float | Unset):
        inclusive_min (bool | Unset):  Default: True.
        inclusive_max (bool | Unset):  Default: False.
    """

    field: str
    min_: float | Unset = UNSET
    max_: float | Unset = UNSET
    inclusive_min: bool | Unset = True
    inclusive_max: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        min_ = self.min_

        max_ = self.max_

        inclusive_min = self.inclusive_min

        inclusive_max = self.inclusive_max

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if min_ is not UNSET:
            field_dict["min"] = min_
        if max_ is not UNSET:
            field_dict["max"] = max_
        if inclusive_min is not UNSET:
            field_dict["inclusive_min"] = inclusive_min
        if inclusive_max is not UNSET:
            field_dict["inclusive_max"] = inclusive_max

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        min_ = d.pop("min", UNSET)

        max_ = d.pop("max", UNSET)

        inclusive_min = d.pop("inclusive_min", UNSET)

        inclusive_max = d.pop("inclusive_max", UNSET)

        graph_document_numeric_range_body = cls(
            field=field,
            min_=min_,
            max_=max_,
            inclusive_min=inclusive_min,
            inclusive_max=inclusive_max,
        )

        return graph_document_numeric_range_body
