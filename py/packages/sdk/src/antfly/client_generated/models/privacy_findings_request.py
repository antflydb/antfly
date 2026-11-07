from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PrivacyFindingsRequest")


@_attrs_define
class PrivacyFindingsRequest:
    """Findings filters and opaque continuation in the request body, avoiding URL limits for large document identifiers.

    Attributes:
        cursor (str | Unset):
        document (str | Unset):
        field (str | Unset):
        category (str | Unset):
        policy_version (str | Unset):
        limit (int | Unset):  Default: 100.
    """

    cursor: str | Unset = UNSET
    document: str | Unset = UNSET
    field: str | Unset = UNSET
    category: str | Unset = UNSET
    policy_version: str | Unset = UNSET
    limit: int | Unset = 100

    def to_dict(self) -> dict[str, Any]:
        cursor = self.cursor

        document = self.document

        field = self.field

        category = self.category

        policy_version = self.policy_version

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if document is not UNSET:
            field_dict["document"] = document
        if field is not UNSET:
            field_dict["field"] = field
        if category is not UNSET:
            field_dict["category"] = category
        if policy_version is not UNSET:
            field_dict["policy_version"] = policy_version
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        cursor = d.pop("cursor", UNSET)

        document = d.pop("document", UNSET)

        field = d.pop("field", UNSET)

        category = d.pop("category", UNSET)

        policy_version = d.pop("policy_version", UNSET)

        limit = d.pop("limit", UNSET)

        privacy_findings_request = cls(
            cursor=cursor,
            document=document,
            field=field,
            category=category,
            policy_version=policy_version,
            limit=limit,
        )

        return privacy_findings_request
