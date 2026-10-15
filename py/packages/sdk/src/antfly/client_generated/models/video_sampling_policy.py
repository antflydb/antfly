from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.video_sampling_policy_overflow import VideoSamplingPolicyOverflow
from ..models.video_sampling_policy_policy import VideoSamplingPolicyPolicy
from ..types import UNSET, Unset

T = TypeVar("T", bound="VideoSamplingPolicy")


@_attrs_define
class VideoSamplingPolicy:
    """Explicit EmbeddingGemma 2 video-group sampling. Timestamp windows are half-open and relative to the first
    presentation picture.

        Attributes:
            policy (VideoSamplingPolicyPolicy):
            start_ns (int | Unset):
            end_ns (int | Unset):
            step_ns (int | Unset):
            max_frames (int | Unset):
            overflow (VideoSamplingPolicyOverflow | Unset):
            hold (bool | Unset): Sample the displayed picture, including VFR holds, rather than the next picture at or after
                the target.
    """

    policy: VideoSamplingPolicyPolicy
    start_ns: int | Unset = UNSET
    end_ns: int | Unset = UNSET
    step_ns: int | Unset = UNSET
    max_frames: int | Unset = UNSET
    overflow: VideoSamplingPolicyOverflow | Unset = UNSET
    hold: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        policy = self.policy.value

        start_ns = self.start_ns

        end_ns = self.end_ns

        step_ns = self.step_ns

        max_frames = self.max_frames

        overflow: str | Unset = UNSET
        if not isinstance(self.overflow, Unset):
            overflow = self.overflow.value

        hold = self.hold

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "policy": policy,
            }
        )
        if start_ns is not UNSET:
            field_dict["start_ns"] = start_ns
        if end_ns is not UNSET:
            field_dict["end_ns"] = end_ns
        if step_ns is not UNSET:
            field_dict["step_ns"] = step_ns
        if max_frames is not UNSET:
            field_dict["max_frames"] = max_frames
        if overflow is not UNSET:
            field_dict["overflow"] = overflow
        if hold is not UNSET:
            field_dict["hold"] = hold

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        policy = VideoSamplingPolicyPolicy(d.pop("policy"))

        start_ns = d.pop("start_ns", UNSET)

        end_ns = d.pop("end_ns", UNSET)

        step_ns = d.pop("step_ns", UNSET)

        max_frames = d.pop("max_frames", UNSET)

        _overflow = d.pop("overflow", UNSET)
        overflow: VideoSamplingPolicyOverflow | Unset
        if isinstance(_overflow, Unset):
            overflow = UNSET
        else:
            overflow = VideoSamplingPolicyOverflow(_overflow)

        hold = d.pop("hold", UNSET)

        video_sampling_policy = cls(
            policy=policy,
            start_ns=start_ns,
            end_ns=end_ns,
            step_ns=step_ns,
            max_frames=max_frames,
            overflow=overflow,
            hold=hold,
        )

        video_sampling_policy.additional_properties = d
        return video_sampling_policy

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
