from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.privacy_finding import PrivacyFinding


T = TypeVar("T", bound="PrivacyFindingsPage")


@_attrs_define
class PrivacyFindingsPage:
    """
    Attributes:
        policy_version (str):
        findings (list[PrivacyFinding]):
        next_cursor (None | str | Unset): Opaque continuation bound to the table, policy version, and filters. Pass it
            unchanged; its size depends on the document identifier.
        examined_documents (int | Unset):
        pending_documents (int | Unset):
    """

    policy_version: str
    findings: list[PrivacyFinding]
    next_cursor: None | str | Unset = UNSET
    examined_documents: int | Unset = UNSET
    pending_documents: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        policy_version = self.policy_version

        findings = []
        for findings_item_data in self.findings:
            findings_item = findings_item_data.to_dict()
            findings.append(findings_item)

        next_cursor: None | str | Unset
        if isinstance(self.next_cursor, Unset):
            next_cursor = UNSET
        else:
            next_cursor = self.next_cursor

        examined_documents = self.examined_documents

        pending_documents = self.pending_documents

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "policy_version": policy_version,
                "findings": findings,
            }
        )
        if next_cursor is not UNSET:
            field_dict["next_cursor"] = next_cursor
        if examined_documents is not UNSET:
            field_dict["examined_documents"] = examined_documents
        if pending_documents is not UNSET:
            field_dict["pending_documents"] = pending_documents

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_finding import PrivacyFinding

        d = dict(src_dict)
        policy_version = d.pop("policy_version")

        findings = []
        _findings = d.pop("findings")
        for findings_item_data in _findings:
            findings_item = PrivacyFinding.from_dict(findings_item_data)

            findings.append(findings_item)

        def _parse_next_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor", UNSET))

        examined_documents = d.pop("examined_documents", UNSET)

        pending_documents = d.pop("pending_documents", UNSET)

        privacy_findings_page = cls(
            policy_version=policy_version,
            findings=findings,
            next_cursor=next_cursor,
            examined_documents=examined_documents,
            pending_documents=pending_documents,
        )

        privacy_findings_page.additional_properties = d
        return privacy_findings_page

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
