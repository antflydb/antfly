from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="TranscriptSpanSeed")


@_attrs_define
class TranscriptSpanSeed:
    """
    Attributes:
        char_start (int):
        char_end (int):
        start_ms (int):
        end_ms (int):
        speaker (int | str | Unset): `SPEAKER_NN` or a speaker index.
    """

    char_start: int
    char_end: int
    start_ms: int
    end_ms: int
    speaker: int | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        char_start = self.char_start

        char_end = self.char_end

        start_ms = self.start_ms

        end_ms = self.end_ms

        speaker: int | str | Unset
        if isinstance(self.speaker, Unset):
            speaker = UNSET
        else:
            speaker = self.speaker

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "char_start": char_start,
                "char_end": char_end,
                "start_ms": start_ms,
                "end_ms": end_ms,
            }
        )
        if speaker is not UNSET:
            field_dict["speaker"] = speaker

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        char_start = d.pop("char_start")

        char_end = d.pop("char_end")

        start_ms = d.pop("start_ms")

        end_ms = d.pop("end_ms")

        def _parse_speaker(data: object) -> int | str | Unset:
            if isinstance(data, Unset):
                return data
            return cast(int | str | Unset, data)

        speaker = _parse_speaker(d.pop("speaker", UNSET))

        transcript_span_seed = cls(
            char_start=char_start,
            char_end=char_end,
            start_ms=start_ms,
            end_ms=end_ms,
            speaker=speaker,
        )

        return transcript_span_seed
