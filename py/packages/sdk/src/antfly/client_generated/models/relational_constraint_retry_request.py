from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RelationalConstraintRetryRequest")


@_attrs_define
class RelationalConstraintRetryRequest:
    """
    Attributes:
        schema_version (int):
    """

    schema_version: int

    def to_dict(self) -> dict[str, Any]:
        schema_version = self.schema_version

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "schema_version": schema_version,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        schema_version = d.pop("schema_version")

        relational_constraint_retry_request = cls(
            schema_version=schema_version,
        )

        return relational_constraint_retry_request
