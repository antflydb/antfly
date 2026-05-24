from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.auth_subject_kind import AuthSubjectKind

T = TypeVar("T", bound="AuthSubject")


@_attrs_define
class AuthSubject:
    """
    Attributes:
        subject (str): Casbin subject name. Example: role:tenant_reader.
        kind (AuthSubjectKind): Conservative subject classification inferred from user records and subject prefixes.
            Example: role.
    """

    subject: str
    kind: AuthSubjectKind
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        subject = self.subject

        kind = self.kind.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "subject": subject,
                "kind": kind,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        subject = d.pop("subject")

        kind = AuthSubjectKind(d.pop("kind"))

        auth_subject = cls(
            subject=subject,
            kind=kind,
        )

        auth_subject.additional_properties = d
        return auth_subject

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
