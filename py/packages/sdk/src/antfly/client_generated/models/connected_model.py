from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.connected_model_qualified_limits import ConnectedModelQualifiedLimits


T = TypeVar("T", bound="ConnectedModel")


@_attrs_define
class ConnectedModel:
    """
    Attributes:
        name (str): Model identifier as reported by the provider.
        display_name (str | Unset): Human-readable model name when the provider reports one.
        dimensions (int | Unset): Embedding output dimension when known.
        configured (bool | Unset): True when this model is referenced by a configured embedder, generator, reranker, or
            chunker.
        capabilities (list[str] | Unset): Inference capabilities advertised by the provider for this model and task.
        inputs (list[str] | Unset): Input modalities accepted by the provider's resolved executor.
        qualified_limits (ConnectedModelQualifiedLimits | Unset): Pull-time qualified request profile for this exact
            artifact, when available.
        allow_downloads (bool | Unset): Whether this provider permits clients to show model download commands.
    """

    name: str
    display_name: str | Unset = UNSET
    dimensions: int | Unset = UNSET
    configured: bool | Unset = UNSET
    capabilities: list[str] | Unset = UNSET
    inputs: list[str] | Unset = UNSET
    qualified_limits: ConnectedModelQualifiedLimits | Unset = UNSET
    allow_downloads: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        display_name = self.display_name

        dimensions = self.dimensions

        configured = self.configured

        capabilities: list[str] | Unset = UNSET
        if not isinstance(self.capabilities, Unset):
            capabilities = self.capabilities

        inputs: list[str] | Unset = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = self.inputs

        qualified_limits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.qualified_limits, Unset):
            qualified_limits = self.qualified_limits.to_dict()

        allow_downloads = self.allow_downloads

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if display_name is not UNSET:
            field_dict["display_name"] = display_name
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if configured is not UNSET:
            field_dict["configured"] = configured
        if capabilities is not UNSET:
            field_dict["capabilities"] = capabilities
        if inputs is not UNSET:
            field_dict["inputs"] = inputs
        if qualified_limits is not UNSET:
            field_dict["qualified_limits"] = qualified_limits
        if allow_downloads is not UNSET:
            field_dict["allow_downloads"] = allow_downloads

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.connected_model_qualified_limits import ConnectedModelQualifiedLimits

        d = dict(src_dict)
        name = d.pop("name")

        display_name = d.pop("display_name", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        configured = d.pop("configured", UNSET)

        capabilities = cast(list[str], d.pop("capabilities", UNSET))

        inputs = cast(list[str], d.pop("inputs", UNSET))

        _qualified_limits = d.pop("qualified_limits", UNSET)
        qualified_limits: ConnectedModelQualifiedLimits | Unset
        if isinstance(_qualified_limits, Unset):
            qualified_limits = UNSET
        else:
            qualified_limits = ConnectedModelQualifiedLimits.from_dict(_qualified_limits)

        allow_downloads = d.pop("allow_downloads", UNSET)

        connected_model = cls(
            name=name,
            display_name=display_name,
            dimensions=dimensions,
            configured=configured,
            capabilities=capabilities,
            inputs=inputs,
            qualified_limits=qualified_limits,
            allow_downloads=allow_downloads,
        )

        connected_model.additional_properties = d
        return connected_model

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
