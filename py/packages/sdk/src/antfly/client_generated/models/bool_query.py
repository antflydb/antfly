from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.bool_query_body import BoolQueryBody


T = TypeVar("T", bound="BoolQuery")


@_attrs_define
class BoolQuery:
    """Canonical Boolean wrapper. Must clauses score; filter clauses constrain without scoring.

    Attributes:
        bool_ (BoolQueryBody):
    """

    bool_: BoolQueryBody

    def to_dict(self) -> dict[str, Any]:
        bool_ = self.bool_.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "bool": bool_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bool_query_body import BoolQueryBody

        if set(src_dict) - {
            "bool",
        }:
            raise ValueError("Unexpected properties for BoolQuery")
        d = dict(src_dict)
        bool_ = BoolQueryBody.from_dict(d.pop("bool"))

        bool_query = cls(
            bool_=bool_,
        )

        return bool_query
