from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceModelInfoChunkingLimits")


@_attrs_define
class InferenceModelInfoChunkingLimits:
    """Effective model-aware chunk operational ceilings for working/output/event
    bytes, chunk/dependency/index counts and remote fetched bytes, requests
    and cache bytes. Independent of model token limits and object length.
    Configure through NodeConfig.chunking_limits or ANTFLY_CHUNK_<NAME>.
    Scoped and unscoped embedding discovery expose the same values.

    """

    additional_properties: dict[str, int] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        inference_model_info_chunking_limits = cls()

        inference_model_info_chunking_limits.additional_properties = d
        return inference_model_info_chunking_limits

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> int:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: int) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
