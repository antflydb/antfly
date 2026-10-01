from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RenameCatalogResourceRequest")


@_attrs_define
class RenameCatalogResourceRequest:
    """
    Attributes:
        name (str): New logical name. The durable resource identity remains unchanged.
    """

    name: str

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        rename_catalog_resource_request = cls(
            name=name,
        )

        return rename_catalog_resource_request
