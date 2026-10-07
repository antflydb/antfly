from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_decide_long_document import InferenceDecideLongDocument
    from ..models.inference_decide_request_questions import InferenceDecideRequestQuestions


T = TypeVar("T", bound="InferenceDecideRequest")


@_attrs_define
class InferenceDecideRequest:
    """
    Attributes:
        model (str):
        state (str):
        questions (InferenceDecideRequestQuestions):
        long_document (InferenceDecideLongDocument | Unset): Explicit windowing for qualified boundary decision models.
            Span decision models use their native context and reject window mode. Omission preserves rejection of over-limit
            text.
    """

    model: str
    state: str
    questions: InferenceDecideRequestQuestions
    long_document: InferenceDecideLongDocument | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        state = self.state

        questions = self.questions.to_dict()

        long_document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.long_document, Unset):
            long_document = self.long_document.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "state": state,
                "questions": questions,
            }
        )
        if long_document is not UNSET:
            field_dict["long_document"] = long_document

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_decide_long_document import InferenceDecideLongDocument
        from ..models.inference_decide_request_questions import InferenceDecideRequestQuestions

        d = dict(src_dict)
        model = d.pop("model")

        state = d.pop("state")

        questions = InferenceDecideRequestQuestions.from_dict(d.pop("questions"))

        _long_document = d.pop("long_document", UNSET)
        long_document: InferenceDecideLongDocument | Unset
        if isinstance(_long_document, Unset):
            long_document = UNSET
        else:
            long_document = InferenceDecideLongDocument.from_dict(_long_document)

        inference_decide_request = cls(
            model=model,
            state=state,
            questions=questions,
            long_document=long_document,
        )

        return inference_decide_request
