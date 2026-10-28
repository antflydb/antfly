from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryAdmissionConfig")


@_attrs_define
class QueryAdmissionConfig:
    """
    Attributes:
        max_concurrent_requests (int | Unset): Maximum concurrent query, search, and retrieval requests in this
            process. The default is 32. The budget is shared by REST, MCP,
            retrieval-agent, A2A, and direct API-kernel execution. Full-text,
            vector, hybrid, graph, aggregation, federated searches, and document
            scans consume it. Point lookups and operational/control-plane reads
            remain outside it. Excess HTTP work is rejected immediately with
            HTTP 429 and Retry-After: 1; asynchronous protocols use their native failure
            response. Excess work is not queued. Set to 0 to disable query
            admission. This budget is independent of transport safeguards,
            write admission, and
            resources.inference.admission.max_concurrent_requests.
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

        query_admission_config = cls(
            max_concurrent_requests=max_concurrent_requests,
        )

        return query_admission_config
