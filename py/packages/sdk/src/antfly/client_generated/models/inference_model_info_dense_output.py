from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceModelInfoDenseOutput")


@_attrs_define
class InferenceModelInfoDenseOutput:
    """Proven native dense output width and inclusive supported truncation range, read from resolved metadata without
    loading weights. Omitted when unknown or for non-embedding tasks.

        Attributes:
            native_dimensions (int):
            minimum_dimensions (int):
    """

    native_dimensions: int
    minimum_dimensions: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        native_dimensions = self.native_dimensions

        minimum_dimensions = self.minimum_dimensions

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "native_dimensions": native_dimensions,
                "minimum_dimensions": minimum_dimensions,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        native_dimensions = d.pop("native_dimensions")

        minimum_dimensions = d.pop("minimum_dimensions")

        inference_model_info_dense_output = cls(
            native_dimensions=native_dimensions,
            minimum_dimensions=minimum_dimensions,
        )

        inference_model_info_dense_output.additional_properties = d
        return inference_model_info_dense_output

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
