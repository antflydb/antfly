from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.curation_origin import CurationOrigin


T = TypeVar("T", bound="ResolverRedirect")


@_attrs_define
class ResolverRedirect:
    """
    Attributes:
        key (str):
        target (str):
        origin (CurationOrigin | Unset): Server-owned provenance. Clients supply actor and reason on writes. Zero
            time/revision denote legacy records whose creation is unknown.
    """

    key: str
    target: str
    origin: CurationOrigin | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        target = self.target

        origin: dict[str, Any] | Unset = UNSET
        if not isinstance(self.origin, Unset):
            origin = self.origin.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "target": target,
            }
        )
        if origin is not UNSET:
            field_dict["origin"] = origin

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.curation_origin import CurationOrigin

        d = dict(src_dict)
        key = d.pop("key")

        target = d.pop("target")

        _origin = d.pop("origin", UNSET)
        origin: CurationOrigin | Unset
        if isinstance(_origin, Unset):
            origin = UNSET
        else:
            origin = CurationOrigin.from_dict(_origin)

        resolver_redirect = cls(
            key=key,
            target=target,
            origin=origin,
        )

        resolver_redirect.additional_properties = d
        return resolver_redirect

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
