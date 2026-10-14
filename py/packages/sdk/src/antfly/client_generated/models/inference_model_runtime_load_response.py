from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_runtime_load_response_outcome import InferenceModelRuntimeLoadResponseOutcome

if TYPE_CHECKING:
    from ..models.inference_model_runtime_model import InferenceModelRuntimeModel


T = TypeVar("T", bound="InferenceModelRuntimeLoadResponse")


@_attrs_define
class InferenceModelRuntimeLoadResponse:
    """
    Attributes:
        outcome (InferenceModelRuntimeLoadResponseOutcome):
        model (InferenceModelRuntimeModel):
    """

    outcome: InferenceModelRuntimeLoadResponseOutcome
    model: InferenceModelRuntimeModel
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        outcome = self.outcome.value

        model = self.model.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "outcome": outcome,
                "model": model,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_model_runtime_model import InferenceModelRuntimeModel

        d = dict(src_dict)
        outcome = InferenceModelRuntimeLoadResponseOutcome(d.pop("outcome"))

        model = InferenceModelRuntimeModel.from_dict(d.pop("model"))

        inference_model_runtime_load_response = cls(
            outcome=outcome,
            model=model,
        )

        inference_model_runtime_load_response.additional_properties = d
        return inference_model_runtime_load_response

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
