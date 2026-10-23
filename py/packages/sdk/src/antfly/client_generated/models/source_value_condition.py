from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SourceValueCondition")


@_attrs_define
class SourceValueCondition:
    """
    Attributes:
        id (str):
        identity (str): Opaque versioned identity previously returned by this server for these exact logical bytes.
    """

    id: str
    identity: str

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        identity = self.identity

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "identity": identity,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        identity = d.pop("identity")

        source_value_condition = cls(
            id=id,
            identity=identity,
        )

        return source_value_condition
