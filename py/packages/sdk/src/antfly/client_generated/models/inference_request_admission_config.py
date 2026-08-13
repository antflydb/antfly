from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceRequestAdmissionConfig")


@_attrs_define
class InferenceRequestAdmissionConfig:
    """
    Attributes:
        max_concurrent_requests (int | Unset): Maximum concurrent inference requests in this process. The same
            value also sizes the weighted work budget shared by every executing
            inference endpoint: each request consumes one request slot and at
            least one unit, while request body size, generation workload, and
            image byte/count reservations can consume more than one unit. The
            request count can therefore never exceed this value, while expensive
            requests may exhaust weighted capacity sooner. Read and
            image-extraction admission reserves the effective downloaded-byte ceiling at 16 MiB
            per unit and at least one unit per two images. A positive capacity also
            clamps each such request's downloaded-image ceiling to 16 MiB times
            this value. When either ceiling is exhausted, new HTTP requests are
            rejected immediately with 503 Service Unavailable and Retry-After: 1;
            they are not retained
            in an in-process queue. Set to 0 to disable both ceilings, admission
            unit accounting, and the capacity-derived clamp. The default is 32.
             Default: 32. Example: 32.
    """

    max_concurrent_requests: int | Unset = 32

    def to_dict(self) -> dict[str, Any]:
        max_concurrent_requests = self.max_concurrent_requests

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_concurrent_requests is not UNSET:
            field_dict["max_concurrent_requests"] = max_concurrent_requests

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_concurrent_requests = d.pop("max_concurrent_requests", UNSET)

        inference_request_admission_config = cls(
            max_concurrent_requests=max_concurrent_requests,
        )

        return inference_request_admission_config
