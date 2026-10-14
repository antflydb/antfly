from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_model_kind import InferenceModelKind

T = TypeVar("T", bound="InferenceModelRuntimeLoadRequest")


@_attrs_define
class InferenceModelRuntimeLoadRequest:
    """
    Attributes:
        task (InferenceModelKind): Model registry kind.
        model (str):
    """

    task: InferenceModelKind
    model: str

    def to_dict(self) -> dict[str, Any]:
        task = self.task.value

        model = self.model

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "task": task,
                "model": model,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        task = InferenceModelKind(d.pop("task"))

        model = d.pop("model")

        inference_model_runtime_load_request = cls(
            task=task,
            model=model,
        )

        return inference_model_runtime_load_request
