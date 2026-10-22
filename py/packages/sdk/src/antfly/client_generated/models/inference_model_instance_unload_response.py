from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_instance_unload_response_outcome import InferenceModelInstanceUnloadResponseOutcome

T = TypeVar("T", bound="InferenceModelInstanceUnloadResponse")


@_attrs_define
class InferenceModelInstanceUnloadResponse:
    """
    Attributes:
        outcome (InferenceModelInstanceUnloadResponseOutcome):
    """

    outcome: InferenceModelInstanceUnloadResponseOutcome
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        outcome = self.outcome.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "outcome": outcome,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        outcome = InferenceModelInstanceUnloadResponseOutcome(d.pop("outcome"))

        inference_model_instance_unload_response = cls(
            outcome=outcome,
        )

        inference_model_instance_unload_response.additional_properties = d
        return inference_model_instance_unload_response

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
