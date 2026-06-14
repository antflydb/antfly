from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extension_scope_kind import ExtensionScopeKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.data_shape_decl import DataShapeDecl
    from ..models.extension_object_decl import ExtensionObjectDecl
    from ..models.runtime_decl import RuntimeDecl


T = TypeVar("T", bound="InstallManifest")


@_attrs_define
class InstallManifest:
    """
    Attributes:
        scopes_supported (list[ExtensionScopeKind]):
        shapes (list[DataShapeDecl] | Unset):
        objects (list[ExtensionObjectDecl] | Unset):
        runtimes (list[RuntimeDecl] | Unset):
        config_schema_json (str | Unset):  Default: '{}'.
    """

    scopes_supported: list[ExtensionScopeKind]
    shapes: list[DataShapeDecl] | Unset = UNSET
    objects: list[ExtensionObjectDecl] | Unset = UNSET
    runtimes: list[RuntimeDecl] | Unset = UNSET
    config_schema_json: str | Unset = "{}"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        scopes_supported = []
        for scopes_supported_item_data in self.scopes_supported:
            scopes_supported_item = scopes_supported_item_data.value
            scopes_supported.append(scopes_supported_item)

        shapes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.shapes, Unset):
            shapes = []
            for shapes_item_data in self.shapes:
                shapes_item = shapes_item_data.to_dict()
                shapes.append(shapes_item)

        objects: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.objects, Unset):
            objects = []
            for objects_item_data in self.objects:
                objects_item = objects_item_data.to_dict()
                objects.append(objects_item)

        runtimes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.runtimes, Unset):
            runtimes = []
            for runtimes_item_data in self.runtimes:
                runtimes_item = runtimes_item_data.to_dict()
                runtimes.append(runtimes_item)

        config_schema_json = self.config_schema_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "scopes_supported": scopes_supported,
            }
        )
        if shapes is not UNSET:
            field_dict["shapes"] = shapes
        if objects is not UNSET:
            field_dict["objects"] = objects
        if runtimes is not UNSET:
            field_dict["runtimes"] = runtimes
        if config_schema_json is not UNSET:
            field_dict["config_schema_json"] = config_schema_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.data_shape_decl import DataShapeDecl
        from ..models.extension_object_decl import ExtensionObjectDecl
        from ..models.runtime_decl import RuntimeDecl

        d = dict(src_dict)
        scopes_supported = []
        _scopes_supported = d.pop("scopes_supported")
        for scopes_supported_item_data in _scopes_supported:
            scopes_supported_item = ExtensionScopeKind(scopes_supported_item_data)

            scopes_supported.append(scopes_supported_item)

        _shapes = d.pop("shapes", UNSET)
        shapes: list[DataShapeDecl] | Unset = UNSET
        if _shapes is not UNSET:
            shapes = []
            for shapes_item_data in _shapes:
                shapes_item = DataShapeDecl.from_dict(shapes_item_data)

                shapes.append(shapes_item)

        _objects = d.pop("objects", UNSET)
        objects: list[ExtensionObjectDecl] | Unset = UNSET
        if _objects is not UNSET:
            objects = []
            for objects_item_data in _objects:
                objects_item = ExtensionObjectDecl.from_dict(objects_item_data)

                objects.append(objects_item)

        _runtimes = d.pop("runtimes", UNSET)
        runtimes: list[RuntimeDecl] | Unset = UNSET
        if _runtimes is not UNSET:
            runtimes = []
            for runtimes_item_data in _runtimes:
                runtimes_item = RuntimeDecl.from_dict(runtimes_item_data)

                runtimes.append(runtimes_item)

        config_schema_json = d.pop("config_schema_json", UNSET)

        install_manifest = cls(
            scopes_supported=scopes_supported,
            shapes=shapes,
            objects=objects,
            runtimes=runtimes,
            config_schema_json=config_schema_json,
        )

        install_manifest.additional_properties = d
        return install_manifest

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
