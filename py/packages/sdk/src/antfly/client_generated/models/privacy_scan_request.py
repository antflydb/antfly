from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PrivacyScanRequest")


@_attrs_define
class PrivacyScanRequest:
    """
    Attributes:
        from_key (str | Unset):
        to_key (str | Unset):
        limit (int | Unset):  Default: 100.
    """

    from_key: str | Unset = UNSET
    to_key: str | Unset = UNSET
    limit: int | Unset = 100

    def to_dict(self) -> dict[str, Any]:
        from_key = self.from_key

        to_key = self.to_key

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if from_key is not UNSET:
            field_dict["from_key"] = from_key
        if to_key is not UNSET:
            field_dict["to_key"] = to_key
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        from_key = d.pop("from_key", UNSET)

        to_key = d.pop("to_key", UNSET)

        limit = d.pop("limit", UNSET)

        privacy_scan_request = cls(
            from_key=from_key,
            to_key=to_key,
            limit=limit,
        )

        return privacy_scan_request
