from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="MediaChunkPolicy")


@_attrs_define
class MediaChunkPolicy:
    """Semantic chunk policy. Execution batch size and queue depth are deliberately separate.

    Attributes:
        max_text_bytes (int | Unset): Explicit byte ceiling per text chunk, independently of the target tokenizer
            budget. Default: 1048576.
        target_tokens (int | Unset):  Default: 500.
        overlap_tokens (int | Unset):  Default: 50.
        window_ns (int | Unset):  Default: 32000000000.
        overlap_ns (int | Unset):  Default: 0.
        step_ns (int | Unset):  Default: 1000000000.
        video_hold (int | Unset): Select the displayed picture at each video cadence target, including VFR holds. Zero
            retains at-or-after timestamp sampling. Default: 1.
        max_frames (int | Unset):  Default: 32.
        tile_width (int | Unset):  Default: 0.
        tile_height (int | Unset):  Default: 0.
        tile_overlap (int | Unset):  Default: 0.
        audio_speaker_turns (bool | Unset): Split audio chunks at detected speaker transitions, additionally respecting
            the boundary model limits. Requires a diarization-enabled boundary target, zero overlap_ns and zero
            audio_pause_ns. The boundary diarization recipe participates in plan identity. Analysis runs even with
            execute=false because these boundaries depend on audio inference. Default: False.
        audio_pause_ns (int | Unset): Optional minimum silence duration for choosing an earlier boundary; no samples are
            discarded. Default: 0.
        audio_pause_search_ns (int | Unset):  Default: 5000000000.
        audio_silence_milli (int | Unset): Absolute normalized PCM amplitude threshold in thousandths. Default: 10.
    """

    max_text_bytes: int | Unset = 1048576
    target_tokens: int | Unset = 500
    overlap_tokens: int | Unset = 50
    window_ns: int | Unset = 32000000000
    overlap_ns: int | Unset = 0
    step_ns: int | Unset = 1000000000
    video_hold: int | Unset = 1
    max_frames: int | Unset = 32
    tile_width: int | Unset = 0
    tile_height: int | Unset = 0
    tile_overlap: int | Unset = 0
    audio_speaker_turns: bool | Unset = False
    audio_pause_ns: int | Unset = 0
    audio_pause_search_ns: int | Unset = 5000000000
    audio_silence_milli: int | Unset = 10
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        max_text_bytes = self.max_text_bytes

        target_tokens = self.target_tokens

        overlap_tokens = self.overlap_tokens

        window_ns = self.window_ns

        overlap_ns = self.overlap_ns

        step_ns = self.step_ns

        video_hold = self.video_hold

        max_frames = self.max_frames

        tile_width = self.tile_width

        tile_height = self.tile_height

        tile_overlap = self.tile_overlap

        audio_speaker_turns = self.audio_speaker_turns

        audio_pause_ns = self.audio_pause_ns

        audio_pause_search_ns = self.audio_pause_search_ns

        audio_silence_milli = self.audio_silence_milli

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if max_text_bytes is not UNSET:
            field_dict["max_text_bytes"] = max_text_bytes
        if target_tokens is not UNSET:
            field_dict["target_tokens"] = target_tokens
        if overlap_tokens is not UNSET:
            field_dict["overlap_tokens"] = overlap_tokens
        if window_ns is not UNSET:
            field_dict["window_ns"] = window_ns
        if overlap_ns is not UNSET:
            field_dict["overlap_ns"] = overlap_ns
        if step_ns is not UNSET:
            field_dict["step_ns"] = step_ns
        if video_hold is not UNSET:
            field_dict["video_hold"] = video_hold
        if max_frames is not UNSET:
            field_dict["max_frames"] = max_frames
        if tile_width is not UNSET:
            field_dict["tile_width"] = tile_width
        if tile_height is not UNSET:
            field_dict["tile_height"] = tile_height
        if tile_overlap is not UNSET:
            field_dict["tile_overlap"] = tile_overlap
        if audio_speaker_turns is not UNSET:
            field_dict["audio_speaker_turns"] = audio_speaker_turns
        if audio_pause_ns is not UNSET:
            field_dict["audio_pause_ns"] = audio_pause_ns
        if audio_pause_search_ns is not UNSET:
            field_dict["audio_pause_search_ns"] = audio_pause_search_ns
        if audio_silence_milli is not UNSET:
            field_dict["audio_silence_milli"] = audio_silence_milli

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_text_bytes = d.pop("max_text_bytes", UNSET)

        target_tokens = d.pop("target_tokens", UNSET)

        overlap_tokens = d.pop("overlap_tokens", UNSET)

        window_ns = d.pop("window_ns", UNSET)

        overlap_ns = d.pop("overlap_ns", UNSET)

        step_ns = d.pop("step_ns", UNSET)

        video_hold = d.pop("video_hold", UNSET)

        max_frames = d.pop("max_frames", UNSET)

        tile_width = d.pop("tile_width", UNSET)

        tile_height = d.pop("tile_height", UNSET)

        tile_overlap = d.pop("tile_overlap", UNSET)

        audio_speaker_turns = d.pop("audio_speaker_turns", UNSET)

        audio_pause_ns = d.pop("audio_pause_ns", UNSET)

        audio_pause_search_ns = d.pop("audio_pause_search_ns", UNSET)

        audio_silence_milli = d.pop("audio_silence_milli", UNSET)

        media_chunk_policy = cls(
            max_text_bytes=max_text_bytes,
            target_tokens=target_tokens,
            overlap_tokens=overlap_tokens,
            window_ns=window_ns,
            overlap_ns=overlap_ns,
            step_ns=step_ns,
            video_hold=video_hold,
            max_frames=max_frames,
            tile_width=tile_width,
            tile_height=tile_height,
            tile_overlap=tile_overlap,
            audio_speaker_turns=audio_speaker_turns,
            audio_pause_ns=audio_pause_ns,
            audio_pause_search_ns=audio_pause_search_ns,
            audio_silence_milli=audio_silence_milli,
        )

        media_chunk_policy.additional_properties = d
        return media_chunk_policy

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
