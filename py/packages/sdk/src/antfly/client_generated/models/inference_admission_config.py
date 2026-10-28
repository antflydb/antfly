from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceAdmissionConfig")


@_attrs_define
class InferenceAdmissionConfig:
    """
    Attributes:
        max_work_units (int | Unset): Weighted work and media capacity, independent of request count. Omitted derives
            capacity from concurrency; zero disables weighted accounting.
        max_concurrent_requests (int | Unset): Maximum concurrent inference requests admitted by this process.
            The budget covers embedded inference routes, direct providers, and
            public connection inference forwarding. Remote forwarding holds a
            slot for the full upstream request. The runtime-reserved
            `local-inference` connection dispatches in process and is admitted
            once by the destination inference route; configured URLs remain
            remote boundaries even when they alias the local listener.
            Embedded standalone uses one shared coordinator for all of those
            surfaces; API-only runtimes use the same setting as a local forwarding
            gate. Weighted work/media capacity is controlled independently by
            max_work_units; when omitted it derives from max_concurrent_requests.
            Request count zero disables only the request-count gate. An explicit
            max_work_units remains authoritative. Excess work is rejected with
            503 and Retry-After; it is not queued.
             Default: 32. Example: 32.
    """

    max_work_units: int | Unset = UNSET
    max_concurrent_requests: int | Unset = 32

    def to_dict(self) -> dict[str, Any]:
        max_work_units = self.max_work_units

        max_concurrent_requests = self.max_concurrent_requests

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_work_units is not UNSET:
            field_dict["max_work_units"] = max_work_units
        if max_concurrent_requests is not UNSET:
            field_dict["max_concurrent_requests"] = max_concurrent_requests

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_work_units = d.pop("max_work_units", UNSET)

        max_concurrent_requests = d.pop("max_concurrent_requests", UNSET)

        inference_admission_config = cls(
            max_work_units=max_work_units,
            max_concurrent_requests=max_concurrent_requests,
        )

        return inference_admission_config
