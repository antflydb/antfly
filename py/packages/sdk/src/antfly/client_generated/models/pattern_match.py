from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.path_edge import PathEdge
    from ..models.pattern_match_bindings import PatternMatchBindings


T = TypeVar("T", bound="PatternMatch")


@_attrs_define
class PatternMatch:
    """A single match from a pattern query

    Attributes:
        bindings (PatternMatchBindings | Unset): Map of alias to matched node
        path (list[PathEdge] | Unset): Edges traversed in this match
    """

    bindings: PatternMatchBindings | Unset = UNSET
    path: list[PathEdge] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        bindings: dict[str, Any] | Unset = UNSET
        if not isinstance(self.bindings, Unset):
            bindings = self.bindings.to_dict()

        path: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.path, Unset):
            path = []
            for path_item_data in self.path:
                path_item = path_item_data.to_dict()
                path.append(path_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if bindings is not UNSET:
            field_dict["bindings"] = bindings
        if path is not UNSET:
            field_dict["path"] = path

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.path_edge import PathEdge
        from ..models.pattern_match_bindings import PatternMatchBindings

        d = dict(src_dict)
        _bindings = d.pop("bindings", UNSET)
        bindings: PatternMatchBindings | Unset
        if isinstance(_bindings, Unset):
            bindings = UNSET
        else:
            bindings = PatternMatchBindings.from_dict(_bindings)

        _path = d.pop("path", UNSET)
        path: list[PathEdge] | Unset = UNSET
        if _path is not UNSET:
            path = []
            for path_item_data in _path:
                path_item = PathEdge.from_dict(path_item_data)

                path.append(path_item)

        pattern_match = cls(
            bindings=bindings,
            path=path,
        )

        pattern_match.additional_properties = d
        return pattern_match

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
