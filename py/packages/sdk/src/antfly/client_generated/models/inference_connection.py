from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.connected_model_type import ConnectedModelType
from ..models.inference_provider_type import InferenceProviderType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_connection_models import InferenceConnectionModels


T = TypeVar("T", bound="InferenceConnection")


@_attrs_define
class InferenceConnection:
    """
    Attributes:
        provider (InferenceProviderType): Inference provider type for a connection.
        url (str | Unset): Resolved endpoint URL when applicable.
        region (str | Unset): Cloud region (Bedrock).
        project_id (str | Unset): Google Cloud project (Vertex).
        location (str | Unset): Google Cloud location (Vertex).
        names (list[str] | Unset): Named registry entries from node config that resolve to this provider instance.
        configured_model_types (list[ConnectedModelType] | Unset): Model types this instance is configured for.
        models (InferenceConnectionModels | Unset): Models reported by the provider, grouped by model type. Keys are
            pluralized ConnectedModelType values ("embedders", "generators",
            "rerankers", "chunkers", "classifiers", "rewriters",
            "readers", "transcribers", "extractors") plus "other" for models
            the provider's listing API does not classify by task. Populated
            only when the request includes the "models" expansion.
    """

    provider: InferenceProviderType
    url: str | Unset = UNSET
    region: str | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    names: list[str] | Unset = UNSET
    configured_model_types: list[ConnectedModelType] | Unset = UNSET
    models: InferenceConnectionModels | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        url = self.url

        region = self.region

        project_id = self.project_id

        location = self.location

        names: list[str] | Unset = UNSET
        if not isinstance(self.names, Unset):
            names = self.names

        configured_model_types: list[str] | Unset = UNSET
        if not isinstance(self.configured_model_types, Unset):
            configured_model_types = []
            for configured_model_types_item_data in self.configured_model_types:
                configured_model_types_item = configured_model_types_item_data.value
                configured_model_types.append(configured_model_types_item)

        models: dict[str, Any] | Unset = UNSET
        if not isinstance(self.models, Unset):
            models = self.models.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if url is not UNSET:
            field_dict["url"] = url
        if region is not UNSET:
            field_dict["region"] = region
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if names is not UNSET:
            field_dict["names"] = names
        if configured_model_types is not UNSET:
            field_dict["configured_model_types"] = configured_model_types
        if models is not UNSET:
            field_dict["models"] = models

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_connection_models import InferenceConnectionModels

        d = dict(src_dict)
        provider = InferenceProviderType(d.pop("provider"))

        url = d.pop("url", UNSET)

        region = d.pop("region", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        names = cast(list[str], d.pop("names", UNSET))

        _configured_model_types = d.pop("configured_model_types", UNSET)
        configured_model_types: list[ConnectedModelType] | Unset = UNSET
        if _configured_model_types is not UNSET:
            configured_model_types = []
            for configured_model_types_item_data in _configured_model_types:
                configured_model_types_item = ConnectedModelType(configured_model_types_item_data)

                configured_model_types.append(configured_model_types_item)

        _models = d.pop("models", UNSET)
        models: InferenceConnectionModels | Unset
        if isinstance(_models, Unset):
            models = UNSET
        else:
            models = InferenceConnectionModels.from_dict(_models)

        inference_connection = cls(
            provider=provider,
            url=url,
            region=region,
            project_id=project_id,
            location=location,
            names=names,
            configured_model_types=configured_model_types,
            models=models,
        )

        inference_connection.additional_properties = d
        return inference_connection

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
