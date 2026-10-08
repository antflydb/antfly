from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_decide_request_questions import InferenceDecideRequestQuestions
    from ..models.inference_embedding_decision_options import InferenceEmbeddingDecisionOptions


T = TypeVar("T", bound="InferenceDecideRequest")


@_attrs_define
class InferenceDecideRequest:
    """
    Attributes:
        model (str):
        state (str):
        questions (InferenceDecideRequestQuestions):
        model_identity (str | Unset): Pin the exact EmbeddingGemma 2 assets and embedding recipe.
        embedding_options (InferenceEmbeddingDecisionOptions | Unset): EmbeddingGemma 2 similarity decisions only. Other
            models reject these options. Uncalibrated results contain cosine scores and never probabilities.
    """

    model: str
    state: str
    questions: InferenceDecideRequestQuestions
    model_identity: str | Unset = UNSET
    embedding_options: InferenceEmbeddingDecisionOptions | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        state = self.state

        questions = self.questions.to_dict()

        model_identity = self.model_identity

        embedding_options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding_options, Unset):
            embedding_options = self.embedding_options.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "state": state,
                "questions": questions,
            }
        )
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if embedding_options is not UNSET:
            field_dict["embedding_options"] = embedding_options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_decide_request_questions import InferenceDecideRequestQuestions
        from ..models.inference_embedding_decision_options import InferenceEmbeddingDecisionOptions

        d = dict(src_dict)
        model = d.pop("model")

        state = d.pop("state")

        questions = InferenceDecideRequestQuestions.from_dict(d.pop("questions"))

        model_identity = d.pop("model_identity", UNSET)

        _embedding_options = d.pop("embedding_options", UNSET)
        embedding_options: InferenceEmbeddingDecisionOptions | Unset
        if isinstance(_embedding_options, Unset):
            embedding_options = UNSET
        else:
            embedding_options = InferenceEmbeddingDecisionOptions.from_dict(_embedding_options)

        inference_decide_request = cls(
            model=model,
            state=state,
            questions=questions,
            model_identity=model_identity,
            embedding_options=embedding_options,
        )

        return inference_decide_request
