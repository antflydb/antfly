from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_config_model_strategies_additional_property import (
    InferenceConfigModelStrategiesAdditionalProperty,
)

T = TypeVar("T", bound="InferenceConfigModelStrategies")


@_attrs_define
class InferenceConfigModelStrategies:
    """Per-model loading strategy overrides. Maps model names to their loading strategy.
    Models not in this map use the default strategy based on keep_alive:
    - If keep_alive>0 (default "5m"): lazy loading (load on demand, unload after idle)
    - If keep_alive="0": eager loading (load at startup, never unload)

    When a model has strategy "eager" in this map:
    - It is loaded at startup (as part of preload)
    - It is never unloaded, even when keep_alive>0 (pinned in memory)

    This allows mixing eager and lazy models in the same pool.

        Example:
            {'BAAI/bge-small-en-v1.5': 'eager', 'mirth/chonky-mmbert-small-multilingual-1': 'lazy'}

    """

    additional_properties: dict[str, InferenceConfigModelStrategiesAdditionalProperty] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.value

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        inference_config_model_strategies = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = InferenceConfigModelStrategiesAdditionalProperty(prop_dict)

            additional_properties[prop_name] = additional_property

        inference_config_model_strategies.additional_properties = additional_properties
        return inference_config_model_strategies

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> InferenceConfigModelStrategiesAdditionalProperty:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: InferenceConfigModelStrategiesAdditionalProperty) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
