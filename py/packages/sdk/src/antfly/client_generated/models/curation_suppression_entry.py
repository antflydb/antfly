from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.curation_origin import CurationOrigin


T = TypeVar("T", bound="CurationSuppressionEntry")


@_attrs_define
class CurationSuppressionEntry:
    """
    Attributes:
        key (str):
        origin (CurationOrigin): Server-owned provenance. Clients supply actor and reason on writes. Zero time/revision
            denote legacy records whose creation is unknown.
    """

    key: str
    origin: CurationOrigin
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        origin = self.origin.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "origin": origin,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.curation_origin import CurationOrigin

        d = dict(src_dict)
        key = d.pop("key")

        origin = CurationOrigin.from_dict(d.pop("origin"))

        curation_suppression_entry = cls(
            key=key,
            origin=origin,
        )

        curation_suppression_entry.additional_properties = d
        return curation_suppression_entry

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
