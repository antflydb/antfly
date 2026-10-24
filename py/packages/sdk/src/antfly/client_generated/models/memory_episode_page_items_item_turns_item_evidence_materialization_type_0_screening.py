from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEpisodePageItemsItemTurnsItemEvidenceMaterializationType0Screening")


@_attrs_define
class MemoryEpisodePageItemsItemTurnsItemEvidenceMaterializationType0Screening:
    """
    Attributes:
        policy_version (int | Unset):  Default: 1.
        redactions (int | Unset):  Default: 0.
        known_tokens (int | Unset):  Default: 0.
        private_keys (int | Unset):  Default: 0.
        credential_assignments (int | Unset):  Default: 0.
        credential_urls (int | Unset):  Default: 0.
        sensitive_fields (int | Unset):  Default: 0.
        custom (int | Unset):  Default: 0.
    """

    policy_version: int | Unset = 1
    redactions: int | Unset = 0
    known_tokens: int | Unset = 0
    private_keys: int | Unset = 0
    credential_assignments: int | Unset = 0
    credential_urls: int | Unset = 0
    sensitive_fields: int | Unset = 0
    custom: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        policy_version = self.policy_version

        redactions = self.redactions

        known_tokens = self.known_tokens

        private_keys = self.private_keys

        credential_assignments = self.credential_assignments

        credential_urls = self.credential_urls

        sensitive_fields = self.sensitive_fields

        custom = self.custom

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if policy_version is not UNSET:
            field_dict["policy_version"] = policy_version
        if redactions is not UNSET:
            field_dict["redactions"] = redactions
        if known_tokens is not UNSET:
            field_dict["known_tokens"] = known_tokens
        if private_keys is not UNSET:
            field_dict["private_keys"] = private_keys
        if credential_assignments is not UNSET:
            field_dict["credential_assignments"] = credential_assignments
        if credential_urls is not UNSET:
            field_dict["credential_urls"] = credential_urls
        if sensitive_fields is not UNSET:
            field_dict["sensitive_fields"] = sensitive_fields
        if custom is not UNSET:
            field_dict["custom"] = custom

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        policy_version = d.pop("policy_version", UNSET)

        redactions = d.pop("redactions", UNSET)

        known_tokens = d.pop("known_tokens", UNSET)

        private_keys = d.pop("private_keys", UNSET)

        credential_assignments = d.pop("credential_assignments", UNSET)

        credential_urls = d.pop("credential_urls", UNSET)

        sensitive_fields = d.pop("sensitive_fields", UNSET)

        custom = d.pop("custom", UNSET)

        memory_episode_page_items_item_turns_item_evidence_materialization_type_0_screening = cls(
            policy_version=policy_version,
            redactions=redactions,
            known_tokens=known_tokens,
            private_keys=private_keys,
            credential_assignments=credential_assignments,
            credential_urls=credential_urls,
            sensitive_fields=sensitive_fields,
            custom=custom,
        )

        return memory_episode_page_items_item_turns_item_evidence_materialization_type_0_screening
