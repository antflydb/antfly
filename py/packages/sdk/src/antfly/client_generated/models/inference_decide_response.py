from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.inference_decide_response_answers import InferenceDecideResponseAnswers
    from ..models.inference_decide_response_usage import InferenceDecideResponseUsage


T = TypeVar("T", bound="InferenceDecideResponse")


@_attrs_define
class InferenceDecideResponse:
    """
    Attributes:
        model (str):
        answers (InferenceDecideResponseAnswers):
        usage (InferenceDecideResponseUsage):
    """

    model: str
    answers: InferenceDecideResponseAnswers
    usage: InferenceDecideResponseUsage
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        answers = self.answers.to_dict()

        usage = self.usage.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "answers": answers,
                "usage": usage,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_decide_response_answers import InferenceDecideResponseAnswers
        from ..models.inference_decide_response_usage import InferenceDecideResponseUsage

        d = dict(src_dict)
        model = d.pop("model")

        answers = InferenceDecideResponseAnswers.from_dict(d.pop("answers"))

        usage = InferenceDecideResponseUsage.from_dict(d.pop("usage"))

        inference_decide_response = cls(
            model=model,
            answers=answers,
            usage=usage,
        )

        inference_decide_response.additional_properties = d
        return inference_decide_response

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
