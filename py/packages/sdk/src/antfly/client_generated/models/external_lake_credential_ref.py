from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExternalLakeCredentialRef")


@_attrs_define
class ExternalLakeCredentialRef:
    """
    Attributes:
        ref (str): Name of a configured external_io connection with lake_read capability.
        scope (str | Unset): Allowed object prefix relative to the configured bucket or filesystem root.
    """

    ref: str
    scope: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        ref = self.ref

        scope = self.scope

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "ref": ref,
            }
        )
        if scope is not UNSET:
            field_dict["scope"] = scope

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        ref = d.pop("ref")

        scope = d.pop("scope", UNSET)

        external_lake_credential_ref = cls(
            ref=ref,
            scope=scope,
        )

        return external_lake_credential_ref
