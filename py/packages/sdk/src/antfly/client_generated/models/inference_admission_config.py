from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_request_admission_config import InferenceRequestAdmissionConfig


T = TypeVar("T", bound="InferenceAdmissionConfig")


@_attrs_define
class InferenceAdmissionConfig:
    """Process-local foreground request admission settings.

    Attributes:
        inference (InferenceRequestAdmissionConfig | Unset):
    """

    inference: InferenceRequestAdmissionConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        inference: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inference, Unset):
            inference = self.inference.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if inference is not UNSET:
            field_dict["inference"] = inference

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_request_admission_config import InferenceRequestAdmissionConfig

        d = dict(src_dict)
        _inference = d.pop("inference", UNSET)
        inference: InferenceRequestAdmissionConfig | Unset
        if isinstance(_inference, Unset):
            inference = UNSET
        else:
            inference = InferenceRequestAdmissionConfig.from_dict(_inference)

        inference_admission_config = cls(
            inference=inference,
        )

        return inference_admission_config
