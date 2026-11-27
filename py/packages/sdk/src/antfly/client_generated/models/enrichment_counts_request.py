from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentCountsRequest")


@_attrs_define
class EnrichmentCountsRequest:
    """One page of an enrichment output count walk.

    Attributes:
        from_ (str | Unset): Document key to resume from (a previous response's `next`); empty starts at the first key.
            Default: ''.
        limit (int | Unset): Documents to count in this page. Default: 1000.
    """

    from_: str | Unset = ""
    limit: int | Unset = 1000

    def to_dict(self) -> dict[str, Any]:
        from_ = self.from_

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if from_ is not UNSET:
            field_dict["from"] = from_
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        from_ = d.pop("from", UNSET)

        limit = d.pop("limit", UNSET)

        enrichment_counts_request = cls(
            from_=from_,
            limit=limit,
        )

        return enrichment_counts_request
