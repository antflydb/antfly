from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="InferenceModelRuntimeUnloadRequest")


@_attrs_define
class InferenceModelRuntimeUnloadRequest:
    """
    Attributes:
        runtime_id (str):
    """

    runtime_id: str

    def to_dict(self) -> dict[str, Any]:
        runtime_id = self.runtime_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "runtime_id": runtime_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        runtime_id = d.pop("runtime_id")

        inference_model_runtime_unload_request = cls(
            runtime_id=runtime_id,
        )

        return inference_model_runtime_unload_request
