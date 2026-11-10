from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_admission_config import QueryAdmissionConfig
    from ..models.resources_config_query_request_limits import ResourcesConfigQueryRequestLimits


T = TypeVar("T", bound="ResourcesConfigQuery")


@_attrs_define
class ResourcesConfigQuery:
    """
    Attributes:
        admission (QueryAdmissionConfig | Unset):
        request_limits (ResourcesConfigQueryRequestLimits | Unset):
    """

    admission: QueryAdmissionConfig | Unset = UNSET
    request_limits: ResourcesConfigQueryRequestLimits | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        admission: dict[str, Any] | Unset = UNSET
        if not isinstance(self.admission, Unset):
            admission = self.admission.to_dict()

        request_limits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.request_limits, Unset):
            request_limits = self.request_limits.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if admission is not UNSET:
            field_dict["admission"] = admission
        if request_limits is not UNSET:
            field_dict["request_limits"] = request_limits

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_admission_config import QueryAdmissionConfig
        from ..models.resources_config_query_request_limits import ResourcesConfigQueryRequestLimits

        if set(src_dict) - {
            "admission",
            "request_limits",
        }:
            raise ValueError("Unexpected properties for ResourcesConfigQuery")
        d = dict(src_dict)
        _admission = d.pop("admission", UNSET)
        admission: QueryAdmissionConfig | Unset
        if isinstance(_admission, Unset):
            admission = UNSET
        else:
            admission = QueryAdmissionConfig.from_dict(_admission)

        _request_limits = d.pop("request_limits", UNSET)
        request_limits: ResourcesConfigQueryRequestLimits | Unset
        if isinstance(_request_limits, Unset):
            request_limits = UNSET
        else:
            request_limits = ResourcesConfigQueryRequestLimits.from_dict(_request_limits)

        resources_config_query = cls(
            admission=admission,
            request_limits=request_limits,
        )

        return resources_config_query
