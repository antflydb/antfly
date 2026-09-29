from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.inference_decide_request_questions import InferenceDecideRequestQuestions


T = TypeVar("T", bound="InferenceDecideRequest")


@_attrs_define
class InferenceDecideRequest:
    """
    Attributes:
        model (str):
        state (str):
        questions (InferenceDecideRequestQuestions):
    """

    model: str
    state: str
    questions: InferenceDecideRequestQuestions

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        state = self.state

        questions = self.questions.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "state": state,
                "questions": questions,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_decide_request_questions import InferenceDecideRequestQuestions

        d = dict(src_dict)
        model = d.pop("model")

        state = d.pop("state")

        questions = InferenceDecideRequestQuestions.from_dict(d.pop("questions"))

        inference_decide_request = cls(
            model=model,
            state=state,
            questions=questions,
        )

        return inference_decide_request
