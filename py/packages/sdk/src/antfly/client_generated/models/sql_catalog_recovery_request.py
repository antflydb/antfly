from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLCatalogRecoveryRequest")


@_attrs_define
class SQLCatalogRecoveryRequest:
    """
    Attributes:
        recovery_token (str):
    """

    recovery_token: str

    def to_dict(self) -> dict[str, Any]:
        recovery_token = self.recovery_token

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "recovery_token": recovery_token,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        recovery_token = d.pop("recovery_token")

        sql_catalog_recovery_request = cls(
            recovery_token=recovery_token,
        )

        return sql_catalog_recovery_request
