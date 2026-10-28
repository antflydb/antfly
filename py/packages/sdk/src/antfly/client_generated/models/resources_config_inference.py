from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_admission_config import InferenceAdmissionConfig
    from ..models.inference_memory_config import InferenceMemoryConfig
    from ..models.inference_request_limits_config import InferenceRequestLimitsConfig


T = TypeVar("T", bound="ResourcesConfigInference")


@_attrs_define
class ResourcesConfigInference:
    """
    Attributes:
        memory (InferenceMemoryConfig | Unset): Inference memory ceilings in bytes. Omitted or zero values select
            automatic policy; physical capacity and the process envelope remain authoritative.
        admission (InferenceAdmissionConfig | Unset):
        request_limits (InferenceRequestLimitsConfig | Unset): Deployment request ceilings only narrow model
            capabilities. Applied equally to direct calls, HTTP and discovery. Omitted values retain native limits.
    """

    memory: InferenceMemoryConfig | Unset = UNSET
    admission: InferenceAdmissionConfig | Unset = UNSET
    request_limits: InferenceRequestLimitsConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        memory: dict[str, Any] | Unset = UNSET
        if not isinstance(self.memory, Unset):
            memory = self.memory.to_dict()

        admission: dict[str, Any] | Unset = UNSET
        if not isinstance(self.admission, Unset):
            admission = self.admission.to_dict()

        request_limits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.request_limits, Unset):
            request_limits = self.request_limits.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if memory is not UNSET:
            field_dict["memory"] = memory
        if admission is not UNSET:
            field_dict["admission"] = admission
        if request_limits is not UNSET:
            field_dict["request_limits"] = request_limits

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_admission_config import InferenceAdmissionConfig
        from ..models.inference_memory_config import InferenceMemoryConfig
        from ..models.inference_request_limits_config import InferenceRequestLimitsConfig

        d = dict(src_dict)
        _memory = d.pop("memory", UNSET)
        memory: InferenceMemoryConfig | Unset
        if isinstance(_memory, Unset):
            memory = UNSET
        else:
            memory = InferenceMemoryConfig.from_dict(_memory)

        _admission = d.pop("admission", UNSET)
        admission: InferenceAdmissionConfig | Unset
        if isinstance(_admission, Unset):
            admission = UNSET
        else:
            admission = InferenceAdmissionConfig.from_dict(_admission)

        _request_limits = d.pop("request_limits", UNSET)
        request_limits: InferenceRequestLimitsConfig | Unset
        if isinstance(_request_limits, Unset):
            request_limits = UNSET
        else:
            request_limits = InferenceRequestLimitsConfig.from_dict(_request_limits)

        resources_config_inference = cls(
            memory=memory,
            admission=admission,
            request_limits=request_limits,
        )

        return resources_config_inference
