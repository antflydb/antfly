from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.memorilla_diagnostics_gate_policy import MemorillaDiagnosticsGatePolicy
from ..models.memorilla_diagnostics_read_mode import MemorillaDiagnosticsReadMode
from ..models.memorilla_diagnostics_read_pass import MemorillaDiagnosticsReadPass
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memorilla_diagnostic_region import MemorillaDiagnosticRegion
    from ..models.memorilla_diagnostic_timings import MemorillaDiagnosticTimings
    from ..models.memorilla_diagnostic_window import MemorillaDiagnosticWindow


T = TypeVar("T", bound="MemorillaDiagnostics")


@_attrs_define
class MemorillaDiagnostics:
    """hybrid_research profiles with include_diagnostics only: how the span
    hybrid read the request. Fields that do not apply are absent (never
    null). Margins are score - null_score - the bundle's span gate
    threshold; the cascade widens when the narrow margin is below
    cascade_threshold, and the gate opens when margin >= gate_offset.
    Research diagnostics, not a stable contract.

        Attributes:
            gate_open (bool): Equals record_gate open. When true the decoder read the soft rows then the window rows.
            raw_answer (str): The stripped decoder answer before canonicalization and the answer guard.
            read_mode (MemorillaDiagnosticsReadMode | Unset): The read the request ran. all is a span bundle without a
                cascade (every text is read). Absent for a copy-head (non-span) bundle.
            read_pass (MemorillaDiagnosticsReadPass | Unset): Cascade only. The pass whose record the request used.
            widened (bool | Unset): Cascade only. True when auto used the wide pass.
            cascade_threshold (float | Unset): Cascade only. The narrow margin below which auto reads wide.
            narrow_margin (float | Unset): Cascade only. Margin of the narrow pass; absent when it did not run.
            wide_margin (float | Unset): Cascade only. Margin of the wide pass; absent when it did not run.
            narrow_unavailable (str | Unset): Cascade only. Why the narrow read set was skipped at preflight (an error
                name); absent when usable.
            wide_unavailable (str | Unset): Cascade only. Why the wide read set was skipped at preflight; absent when
                usable.
            read_documents (int | Unset): Leading texts the used pass read.
            read_tokens (int | Unset): Copy tokens of the used pass's read set.
            read_budget_tokens (int | Unset): Encoder tokens the wide read budget summed; absent when no budget applied.
            window_rows (int | Unset): Evidence window rows of the used pass (32 narrow, the cascade's width wide).
            score (float | Unset): Best span score (start plus end logit).
            null_score (float | Unset): No-answer score.
            margin (float | Unset): Margin of the record used; the gate compares it with gate_offset.
            gate_policy (MemorillaDiagnosticsGatePolicy | Unset):
            gate_offset (float | Unset): The offset the margin was compared with; absent for the always policy (the gate is
                always open).
            gate_profile (str | Unset): Routed or declared gate profile name, when one applied.
            span (MemorillaDiagnosticRegion | Unset): A region of one caller document, mapped from encoder copy positions
                the way citations are.
            window (MemorillaDiagnosticWindow | Unset):
            timings_ms (MemorillaDiagnosticTimings | Unset): Server-side stage times in milliseconds. Each stage is measured
                from
                the end of the previous one, so it also carries the model-handle and
                integrity checks done since. total spans the whole service call
                (preflight, admission and every stage); queueing before it is not
                included.
    """

    gate_open: bool
    raw_answer: str
    read_mode: MemorillaDiagnosticsReadMode | Unset = UNSET
    read_pass: MemorillaDiagnosticsReadPass | Unset = UNSET
    widened: bool | Unset = UNSET
    cascade_threshold: float | Unset = UNSET
    narrow_margin: float | Unset = UNSET
    wide_margin: float | Unset = UNSET
    narrow_unavailable: str | Unset = UNSET
    wide_unavailable: str | Unset = UNSET
    read_documents: int | Unset = UNSET
    read_tokens: int | Unset = UNSET
    read_budget_tokens: int | Unset = UNSET
    window_rows: int | Unset = UNSET
    score: float | Unset = UNSET
    null_score: float | Unset = UNSET
    margin: float | Unset = UNSET
    gate_policy: MemorillaDiagnosticsGatePolicy | Unset = UNSET
    gate_offset: float | Unset = UNSET
    gate_profile: str | Unset = UNSET
    span: MemorillaDiagnosticRegion | Unset = UNSET
    window: MemorillaDiagnosticWindow | Unset = UNSET
    timings_ms: MemorillaDiagnosticTimings | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        gate_open = self.gate_open

        raw_answer = self.raw_answer

        read_mode: str | Unset = UNSET
        if not isinstance(self.read_mode, Unset):
            read_mode = self.read_mode.value

        read_pass: str | Unset = UNSET
        if not isinstance(self.read_pass, Unset):
            read_pass = self.read_pass.value

        widened = self.widened

        cascade_threshold = self.cascade_threshold

        narrow_margin = self.narrow_margin

        wide_margin = self.wide_margin

        narrow_unavailable = self.narrow_unavailable

        wide_unavailable = self.wide_unavailable

        read_documents = self.read_documents

        read_tokens = self.read_tokens

        read_budget_tokens = self.read_budget_tokens

        window_rows = self.window_rows

        score = self.score

        null_score = self.null_score

        margin = self.margin

        gate_policy: str | Unset = UNSET
        if not isinstance(self.gate_policy, Unset):
            gate_policy = self.gate_policy.value

        gate_offset = self.gate_offset

        gate_profile = self.gate_profile

        span: dict[str, Any] | Unset = UNSET
        if not isinstance(self.span, Unset):
            span = self.span.to_dict()

        window: dict[str, Any] | Unset = UNSET
        if not isinstance(self.window, Unset):
            window = self.window.to_dict()

        timings_ms: dict[str, Any] | Unset = UNSET
        if not isinstance(self.timings_ms, Unset):
            timings_ms = self.timings_ms.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "gate_open": gate_open,
                "raw_answer": raw_answer,
            }
        )
        if read_mode is not UNSET:
            field_dict["read_mode"] = read_mode
        if read_pass is not UNSET:
            field_dict["read_pass"] = read_pass
        if widened is not UNSET:
            field_dict["widened"] = widened
        if cascade_threshold is not UNSET:
            field_dict["cascade_threshold"] = cascade_threshold
        if narrow_margin is not UNSET:
            field_dict["narrow_margin"] = narrow_margin
        if wide_margin is not UNSET:
            field_dict["wide_margin"] = wide_margin
        if narrow_unavailable is not UNSET:
            field_dict["narrow_unavailable"] = narrow_unavailable
        if wide_unavailable is not UNSET:
            field_dict["wide_unavailable"] = wide_unavailable
        if read_documents is not UNSET:
            field_dict["read_documents"] = read_documents
        if read_tokens is not UNSET:
            field_dict["read_tokens"] = read_tokens
        if read_budget_tokens is not UNSET:
            field_dict["read_budget_tokens"] = read_budget_tokens
        if window_rows is not UNSET:
            field_dict["window_rows"] = window_rows
        if score is not UNSET:
            field_dict["score"] = score
        if null_score is not UNSET:
            field_dict["null_score"] = null_score
        if margin is not UNSET:
            field_dict["margin"] = margin
        if gate_policy is not UNSET:
            field_dict["gate_policy"] = gate_policy
        if gate_offset is not UNSET:
            field_dict["gate_offset"] = gate_offset
        if gate_profile is not UNSET:
            field_dict["gate_profile"] = gate_profile
        if span is not UNSET:
            field_dict["span"] = span
        if window is not UNSET:
            field_dict["window"] = window
        if timings_ms is not UNSET:
            field_dict["timings_ms"] = timings_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memorilla_diagnostic_region import MemorillaDiagnosticRegion
        from ..models.memorilla_diagnostic_timings import MemorillaDiagnosticTimings
        from ..models.memorilla_diagnostic_window import MemorillaDiagnosticWindow

        d = dict(src_dict)
        gate_open = d.pop("gate_open")

        raw_answer = d.pop("raw_answer")

        _read_mode = d.pop("read_mode", UNSET)
        read_mode: MemorillaDiagnosticsReadMode | Unset
        if isinstance(_read_mode, Unset):
            read_mode = UNSET
        else:
            read_mode = MemorillaDiagnosticsReadMode(_read_mode)

        _read_pass = d.pop("read_pass", UNSET)
        read_pass: MemorillaDiagnosticsReadPass | Unset
        if isinstance(_read_pass, Unset):
            read_pass = UNSET
        else:
            read_pass = MemorillaDiagnosticsReadPass(_read_pass)

        widened = d.pop("widened", UNSET)

        cascade_threshold = d.pop("cascade_threshold", UNSET)

        narrow_margin = d.pop("narrow_margin", UNSET)

        wide_margin = d.pop("wide_margin", UNSET)

        narrow_unavailable = d.pop("narrow_unavailable", UNSET)

        wide_unavailable = d.pop("wide_unavailable", UNSET)

        read_documents = d.pop("read_documents", UNSET)

        read_tokens = d.pop("read_tokens", UNSET)

        read_budget_tokens = d.pop("read_budget_tokens", UNSET)

        window_rows = d.pop("window_rows", UNSET)

        score = d.pop("score", UNSET)

        null_score = d.pop("null_score", UNSET)

        margin = d.pop("margin", UNSET)

        _gate_policy = d.pop("gate_policy", UNSET)
        gate_policy: MemorillaDiagnosticsGatePolicy | Unset
        if isinstance(_gate_policy, Unset):
            gate_policy = UNSET
        else:
            gate_policy = MemorillaDiagnosticsGatePolicy(_gate_policy)

        gate_offset = d.pop("gate_offset", UNSET)

        gate_profile = d.pop("gate_profile", UNSET)

        _span = d.pop("span", UNSET)
        span: MemorillaDiagnosticRegion | Unset
        if isinstance(_span, Unset):
            span = UNSET
        else:
            span = MemorillaDiagnosticRegion.from_dict(_span)

        _window = d.pop("window", UNSET)
        window: MemorillaDiagnosticWindow | Unset
        if isinstance(_window, Unset):
            window = UNSET
        else:
            window = MemorillaDiagnosticWindow.from_dict(_window)

        _timings_ms = d.pop("timings_ms", UNSET)
        timings_ms: MemorillaDiagnosticTimings | Unset
        if isinstance(_timings_ms, Unset):
            timings_ms = UNSET
        else:
            timings_ms = MemorillaDiagnosticTimings.from_dict(_timings_ms)

        memorilla_diagnostics = cls(
            gate_open=gate_open,
            raw_answer=raw_answer,
            read_mode=read_mode,
            read_pass=read_pass,
            widened=widened,
            cascade_threshold=cascade_threshold,
            narrow_margin=narrow_margin,
            wide_margin=wide_margin,
            narrow_unavailable=narrow_unavailable,
            wide_unavailable=wide_unavailable,
            read_documents=read_documents,
            read_tokens=read_tokens,
            read_budget_tokens=read_budget_tokens,
            window_rows=window_rows,
            score=score,
            null_score=null_score,
            margin=margin,
            gate_policy=gate_policy,
            gate_offset=gate_offset,
            gate_profile=gate_profile,
            span=span,
            window=window,
            timings_ms=timings_ms,
        )

        return memorilla_diagnostics
