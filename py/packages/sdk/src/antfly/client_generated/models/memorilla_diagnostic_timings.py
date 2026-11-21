from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemorillaDiagnosticTimings")


@_attrs_define
class MemorillaDiagnosticTimings:
    """Server-side stage times in milliseconds. Each stage is measured from
    the end of the previous one, so it also carries the model-handle and
    integrity checks done since. total spans the whole service call
    (preflight, admission and every stage); queueing before it is not
    included.

        Attributes:
            pooled (float): Stock-encoder pooled vectors of the texts and the question.
            soft_rows (float): The released reader's 16 soft rows.
            generate (float): Decoder prefill and greedy decoding.
            total (float):
            span_features (float | Unset): The first span pass's per-token encoder features (encoder LoRA at run time).
                Absent for read_mode none.
            span_head (float | Unset): The first pass's span head, including the decoder handle acquisition before it.
                Absent for read_mode none.
            wide_features (float | Unset): The cascade's wide pass features (reusing the narrow pass's leading rows).
                Present only when a wide pass ran after the first pass.
            wide_head (float | Unset): The wide pass's span head. Present only with wide_features.
            window (float | Unset): Read plan, evidence window, path checks and the window's embedding rows. Absent for
                read_mode none.
    """

    pooled: float
    soft_rows: float
    generate: float
    total: float
    span_features: float | Unset = UNSET
    span_head: float | Unset = UNSET
    wide_features: float | Unset = UNSET
    wide_head: float | Unset = UNSET
    window: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        pooled = self.pooled

        soft_rows = self.soft_rows

        generate = self.generate

        total = self.total

        span_features = self.span_features

        span_head = self.span_head

        wide_features = self.wide_features

        wide_head = self.wide_head

        window = self.window

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "pooled": pooled,
                "soft_rows": soft_rows,
                "generate": generate,
                "total": total,
            }
        )
        if span_features is not UNSET:
            field_dict["span_features"] = span_features
        if span_head is not UNSET:
            field_dict["span_head"] = span_head
        if wide_features is not UNSET:
            field_dict["wide_features"] = wide_features
        if wide_head is not UNSET:
            field_dict["wide_head"] = wide_head
        if window is not UNSET:
            field_dict["window"] = window

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        pooled = d.pop("pooled")

        soft_rows = d.pop("soft_rows")

        generate = d.pop("generate")

        total = d.pop("total")

        span_features = d.pop("span_features", UNSET)

        span_head = d.pop("span_head", UNSET)

        wide_features = d.pop("wide_features", UNSET)

        wide_head = d.pop("wide_head", UNSET)

        window = d.pop("window", UNSET)

        memorilla_diagnostic_timings = cls(
            pooled=pooled,
            soft_rows=soft_rows,
            generate=generate,
            total=total,
            span_features=span_features,
            span_head=span_head,
            wide_features=wide_features,
            wide_head=wide_head,
            window=window,
        )

        return memorilla_diagnostic_timings
