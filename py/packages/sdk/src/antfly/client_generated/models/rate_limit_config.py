from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.request_pacing import RequestPacing
from ..types import UNSET, Unset

T = TypeVar("T", bound="RateLimitConfig")


@_attrs_define
class RateLimitConfig:
    """Outbound provider limits shared within one Antfly process by effective
    endpoint, operation, model, credential source, project and region/location.
    Conflicting policies for an active scope are rejected. These limits do
    not coordinate across replicas or infer the provider's account quota.

        Attributes:
            pacing (RequestPacing | Unset): token_bucket limits admission rate while allowing overlapping attempts.
                completion serializes attempts and waits one RPM interval after each
                attempt finishes, including streamed writes and transport failures.
                This prevents delayed connection setup from compressing successful
                request spacing, at the cost of response latency plus one interval per
                request. Requires requests_per_minute and burst=1. Neither mode can
                guarantee zero upstream 429s or coordinate other processes.
            requests_per_minute (int | Unset):
            burst (int | Unset):  Default: 1.
            tokens_per_minute (int | Unset): Conservative text budget: each HTTP attempt reserves its serialized
                UTF-8 body byte count plus the configured generation output cap.
                Reservations are not refunded. A request larger than this budget
                is rejected. This is not provider billing token accounting; media
                requests are not supported with this limit.
            max_concurrency (int | Unset): Maximum in-flight HTTP attempts, held through response completion.
    """

    pacing: RequestPacing | Unset = UNSET
    requests_per_minute: int | Unset = UNSET
    burst: int | Unset = 1
    tokens_per_minute: int | Unset = UNSET
    max_concurrency: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        pacing: str | Unset = UNSET
        if not isinstance(self.pacing, Unset):
            pacing = self.pacing.value

        requests_per_minute = self.requests_per_minute

        burst = self.burst

        tokens_per_minute = self.tokens_per_minute

        max_concurrency = self.max_concurrency

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if pacing is not UNSET:
            field_dict["pacing"] = pacing
        if requests_per_minute is not UNSET:
            field_dict["requests_per_minute"] = requests_per_minute
        if burst is not UNSET:
            field_dict["burst"] = burst
        if tokens_per_minute is not UNSET:
            field_dict["tokens_per_minute"] = tokens_per_minute
        if max_concurrency is not UNSET:
            field_dict["max_concurrency"] = max_concurrency

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _pacing = d.pop("pacing", UNSET)
        pacing: RequestPacing | Unset
        if isinstance(_pacing, Unset):
            pacing = UNSET
        else:
            pacing = RequestPacing(_pacing)

        requests_per_minute = d.pop("requests_per_minute", UNSET)

        burst = d.pop("burst", UNSET)

        tokens_per_minute = d.pop("tokens_per_minute", UNSET)

        max_concurrency = d.pop("max_concurrency", UNSET)

        rate_limit_config = cls(
            pacing=pacing,
            requests_per_minute=requests_per_minute,
            burst=burst,
            tokens_per_minute=tokens_per_minute,
            max_concurrency=max_concurrency,
        )

        return rate_limit_config
