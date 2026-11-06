from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PrivacyPreviewRequestSample")


@_attrs_define
class PrivacyPreviewRequestSample:
    """
    Attributes:
        keys (list[str] | Unset):
        limit (int | Unset):  Default: 10.
    """

    keys: list[str] | Unset = UNSET
    limit: int | Unset = 10

    def to_dict(self) -> dict[str, Any]:
        keys: list[str] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = self.keys

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if keys is not UNSET:
            field_dict["keys"] = keys
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        keys = cast(list[str], d.pop("keys", UNSET))

        limit = d.pop("limit", UNSET)

        privacy_preview_request_sample = cls(
            keys=keys,
            limit=limit,
        )

        return privacy_preview_request_sample
